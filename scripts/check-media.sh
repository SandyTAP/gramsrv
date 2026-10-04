#!/usr/bin/env bash
# Cross-check a gramsrv deployment against a live database: every object the
# database references must exist on disk with the right content digest.
#
# This is the check that catches a half-copied media tree, which is the failure
# mode a plain rsync of data/ produces when a file is truncated.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/check-media.sh [options]

  --repo-dir DIR      deployment root (default: repo root)
  --pg-tools MODE     local or docker (default: docker when docker is available)
  --pg-container NAME container providing psql in docker mode
  --dsn DSN           PostgreSQL DSN (default: read from .env)
  --fast              skip content digest verification (much faster, counts only)
  --help

Examples:
  ./scripts/check-media.sh --pg-container telesrv-postgres
  ./scripts/check-media.sh --fast
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

docker_available() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

if has_flag --help "$@" || [[ $# -eq 0 ]]; then
  usage
  [[ $# -eq 0 ]] && exit 1
  exit 0
fi

if [[ ! -x $binary ]]; then
  command -v go >/dev/null 2>&1 || {
    printf 'check-media.sh: go is required to build %s\n' "$binary" >&2
    exit 1
  }
  (cd "$repo_root" && GOFLAGS=-p=1 go build -o "$binary" ./cmd/gramsrv-backup)
fi

args=()
if ! has_flag --repo-dir "$@"; then
  args+=(--repo-dir "$repo_root")
fi
if ! has_flag --pg-tools "$@" && docker_available; then
  args+=(--pg-tools docker)
fi
# "verify" without --from would have nothing to re-hash, so the media
# cross-check is what this script is really for.
if ! has_flag --check-digests "$@"; then
  args+=(--check-digests)
fi

cd "$repo_root"
exec "$binary" verify "${args[@]}" "$@"
