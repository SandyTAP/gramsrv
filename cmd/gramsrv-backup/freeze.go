package main

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// freezeSet stops the writers for the duration of a backup and always puts them
// back, including when the dump fails or the operator interrupts the command.
//
// A pg_dump is already consistent on its own thanks to MVCC, so freezing is not
// required for database correctness. It is required for a byte consistent media
// tree: data/blobs is written by uploads that a plain tar can catch mid-file.
type freezeSet struct {
	stopped []stopped
	log     io.Writer
}

type stopped struct {
	kind string // "systemd unit" or "container"
	name string
}

func newFreezeSet(log io.Writer) *freezeSet {
	return &freezeSet{log: log}
}

// StopUnit stops a systemd unit and schedules it to be started again.
func (f *freezeSet) StopUnit(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	if err := runCommand("systemctl", "stop", name); err != nil {
		return fmt.Errorf("systemctl stop %s: %w", name, err)
	}
	f.stopped = append(f.stopped, stopped{kind: "systemd unit", name: name})
	fmt.Fprintf(f.log, "stopped systemd unit %s\n", name)
	return nil
}

// StopContainer stops a container and schedules it to be started again.
func (f *freezeSet) StopContainer(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	if err := runCommand("docker", "stop", name); err != nil {
		return fmt.Errorf("docker stop %s: %w", name, err)
	}
	f.stopped = append(f.stopped, stopped{kind: "container", name: name})
	fmt.Fprintf(f.log, "stopped container %s\n", name)
	return nil
}

// Release restarts everything that was stopped, newest first, and reports the
// first failure while still attempting every restart.
func (f *freezeSet) Release() error {
	var failures []error
	for i := len(f.stopped) - 1; i >= 0; i-- {
		item := f.stopped[i]
		var err error
		switch item.kind {
		case "systemd unit":
			err = runCommand("systemctl", "start", item.name)
		case "container":
			err = runCommand("docker", "start", item.name)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("restart %s %s: %w", item.kind, item.name, err))
			continue
		}
		fmt.Fprintf(f.log, "restarted %s %s\n", item.kind, item.name)
	}
	f.stopped = nil
	return errors.Join(failures...)
}

// Stopped reports what is currently frozen, for the operator summary.
func (f *freezeSet) Stopped() []string {
	out := make([]string, 0, len(f.stopped))
	for _, item := range f.stopped {
		out = append(out, item.kind+" "+item.name)
	}
	return out
}

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if text := tail(string(out)); text != "" {
			return fmt.Errorf("%w: %s", err, text)
		}
		return err
	}
	return nil
}

// gitRevision returns the current commit, or "unknown" when git is unavailable
// or the tool runs outside a checkout. Provenance is best effort: a bundle
// without a commit is still a valid bundle.
func gitRevision(dir string) string {
	return gitOutput(dir, "rev-parse", "HEAD")
}

func gitBranch(dir string) string {
	return gitOutput(dir, "rev-parse", "--abbrev-ref", "HEAD")
}

func gitOutput(dir string, args ...string) string {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	value := strings.TrimSpace(string(out))
	if value == "" {
		return "unknown"
	}
	return value
}
