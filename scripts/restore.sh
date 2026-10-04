#!/usr/bin/env bash
# Restore a gramsrv backup bundle produced by ./scripts/backup.sh.
#
# The bundle carries the database, the media and key tree, and the configuration,
# so a restore is: put the repository in place, replay the bundle, then start the
# services. Read this script before running it against a live instance; it will
# refuse to overwrite files unless --force is given.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/restore.sh --from DIR [gramsrv-backup flags]

Runs gramsrv-backup restore with defaults for a monolith deployment:

  --from             bundle directory (required)
  --repo-dir         deployment root receiving .env and data/ (default: repo root)
  --component        server, grammystore or all (default: every component present)
  --dsn              target PostgreSQL DSN (default: read from the restored .env)
  --pg-tools         local or docker (default: docker when docker is available)
  --dry-run          print the plan without writing anything
  --force            overwrite existing files in the data tree

Examples:
  # Inspect what a bundle would do.
  ./scripts/restore.sh --from /srv/backups/20261004 --dry-run

  # Restore the database and the media tree onto this host.
  ./scripts/restore.sh --from /srv/backups/20261004 --force

  # Restore only the store bot database.
  ./scripts/restore.sh --from /srv/backups/20261004 --component grammystore

Run './scripts/restore.sh --help' for the full flag list.
EOF
}

script_dir=$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(CDPATH='' cd -- "$script_dir/.." && pwd)
binary="$repo_root/bin/gramsrv-backup"

has_flag() {
  local name=$1
  shift
  local arg
  for arg in "$@"; do
    [[ $arg == "$name" || $arg == "$name="* ]] && return 0
  done
  return 1
}

build_binary() {
  command -v go >/dev/null 2>&1 || {
    printf 'restore.sh: go is required to build %s\n' "$binary" >&2
    return 1
  }
  printf 'restore.sh: building %s\n' "$binary" >&2
  (cd "$repo_root" && GOFLAGS=-p=1 go build -o "$binary" ./cmd/gramsrv-backup)
}

docker_available() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

if has_flag --help "$@"; then
  usage
  exit 0
fi

[[ -x $binary ]] || build_binary

args=()
if ! has_flag --repo-dir "$@"; then
  args+=(--repo-dir "$repo_root")
fi
if ! has_flag --pg-tools "$@" && docker_available; then
  args+=(--pg-tools docker)
fi

cd "$repo_root"
exec "$binary" restore "${args[@]}" "$@"
