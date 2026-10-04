#!/usr/bin/env bash
# Capture a gramsrv backup bundle with sensible defaults for a monolith
# deployment. Every flag is forwarded to gramsrv-backup unchanged, so anything
# this script does not cover is still available:
#
#   ./scripts/backup.sh --out /srv/backups/$(date -u +%Y%m%d) --component all
#
# The tool reads .env for the database credentials, so nothing secret is passed
# on the command line.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/backup.sh [gramsrv-backup flags]

Builds bin/gramsrv-backup when it is missing, then runs a backup with defaults
sensible for a monolith deployment:

  --out              bundle directory (default: ../gramsrv-backup-<UTC date>)
  --component        server, grammystore or all (default: server)
  --repo-dir         deployment root holding .env and data/ (default: repo root)
  --pg-tools         local or docker (default: docker when docker is available)
  --redis-tools      local or docker (default: docker when docker is available)
  --freeze-units     systemd units to stop for the capture
  --freeze-containers containers to stop for the capture

Examples:
  # Hot backup with no downtime, database and media only.
  ./scripts/backup.sh --component server

  # Consistent capture with the writers stopped, restarted automatically.
  ./scripts/backup.sh --component all \
    --pg-container-server telesrv-postgres \
    --pg-container-grammystore grammystore_postgres_dev \
    --freeze-units gramsrv.service,gramsrv-admin.service \
    --freeze-containers grammystore_app_dev

Run './scripts/backup.sh --help' for the full flag list.
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
    printf 'backup.sh: go is required to build %s\n' "$binary" >&2
    return 1
  }
  printf 'backup.sh: building %s\n' "$binary" >&2
  # Serial compilation keeps peak memory low enough for small hosts.
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
if ! has_flag --out "$@"; then
  args+=(--out "$repo_root/../gramsrv-backup-$(date -u +%Y%m%d)")
fi
if ! has_flag --repo-dir "$@"; then
  args+=(--repo-dir "$repo_root")
fi
if ! has_flag --pg-tools "$@" && docker_available; then
  # Most deployments never install PostgreSQL client tools on the host; the
  # database container already carries a matching pg_dump.
  args+=(--pg-tools docker)
fi
if ! has_flag --redis-tools "$@" && docker_available; then
  args+=(--redis-tools docker)
fi

# The tool must run from the deployment root so it finds .env.
cd "$repo_root"
exec "$binary" backup "${args[@]}" "$@"
