# gramsrv-backup

Capture and restore a gramsrv deployment: PostgreSQL, the media and key tree,
Redis, and the configuration needed to bring the same instance up on another
host.

```bash
go build -o bin/gramsrv-backup ./cmd/gramsrv-backup
```

The operator guide, including a full migration runbook, lives in
[`docs/backup-restore.md`](../../docs/backup-restore.md).

## Subcommands

| Command | Purpose |
| --- | --- |
| `backup` | write a new bundle |
| `restore` | replay a bundle onto this host |
| `verify` | re-hash a bundle, and cross-check a live media tree |

## Quick start

```bash
# Capture with defaults read from .env.
./bin/gramsrv-backup backup --out /srv/backups/20261004

# Capture everything, with the writers stopped for a consistent media tree.
./bin/gramsrv-backup backup --out /srv/backups/20261004 \
  --component all \
  --pg-tools docker --pg-container-server telesrv-postgres \
  --redis-tools docker --redis-container telesrv-redis \
  --freeze-units gramsrv.service,gramsrv-admin.service

# Verify before trusting it.
./bin/gramsrv-backup verify --from /srv/backups/20261004

# Check the media tree of the running instance against the database.
./bin/gramsrv-backup verify --pg-tools docker --pg-container telesrv-postgres --check-digests

# Restore.
./bin/gramsrv-backup restore --from /srv/backups/20261004 --dry-run
./bin/gramsrv-backup restore --from /srv/backups/20261004 --force
```

`./scripts/backup.sh`, `./scripts/restore.sh` and `./scripts/check-media.sh` wrap
these with per-deployment defaults and build the binary when it is missing.

## Components

`-component` selects what is captured and is repeatable:

- `server` (default) — the `telesrv_main` database, Redis, the `data/` tree and
  the telesrv configuration.
- `grammystore` — the store bot database and its configuration, read from
  `cmd/bots/grammystore/.env`.
- `all` — both.

Each component has its own database, and usually its own container, so
`-pg-container-server` and `-pg-container-grammystore` override the shared
`-pg-container`.

## Backup flags worth knowing

| Flag | Effect |
| --- | --- |
| `-out DIR` | bundle directory to create (required) |
| `-component` | `server`, `grammystore`, `all`, repeatable |
| `-pg-tools` | `local` or `docker`; docker uses the container's own `pg_dump` |
| `-pg-container` | container providing `pg_dump` and `psql` in docker mode |
| `-pg-container-server`, `-pg-container-grammystore` | per-component override |
| `-redis-tools`, `-redis-container` | same, for `redis-cli` |
| `-dsn` | override the DSN instead of loading it from `.env` |
| `-data-dir DIR` | media and key tree, when it is not where `.env` says |
| `-freeze-units`, `-freeze-containers` | stop these for the capture and always restart them |
| `-include-data` | archive the `data/` tree (default true) |
| `-include-redis` | capture an RDB snapshot (default true) |
| `-no-role-passwords` | strip password hashes from `globals.sql.zst` |
| `-zstd-level` | 1 fastest to 4 smallest (default 3) |

`-freeze-*` is optional. A `pg_dump` is already consistent without it; freezing
is what makes `data.tar.zst` byte consistent, because uploads cannot land
mid-file.

## Restore flags worth knowing

| Flag | Effect |
| --- | --- |
| `-from DIR` | bundle to replay (required) |
| `-component` | limit the restore to one component |
| `-dsn DSN` | target database; defaults to the one in the restored `.env` |
| `-create-database` | create the target database when missing (default true) |
| `-restore-globals` | replay roles and passwords (default true) |
| `-with-data`, `-with-config` | include the media tree and the configuration |
| `-force` | overwrite existing files; without it the restore refuses to |
| `-dry-run` | print the plan and change nothing |
| `-skip-verify` | skip the checksum pass (not recommended) |

The schema restore runs with `ON_ERROR_STOP=1` and `--single-transaction`, so it
either lands completely or not at all.

## Verify flags worth knowing

| Flag | Effect |
| --- | --- |
| `-from DIR` | bundle to check; omit for a live media check only |
| `-check-archives` | decompress artifacts and walk tar members (default true) |
| `-check-digests` | hash every media file and compare with its name |
| `-dsn`, `-data-dir` | target for the media cross-check; default from `.env` |
| `-verbose` | list every missing, orphan and corrupt object |

## Exit status

`0` on success, `1` on any error, including a checksum mismatch, an unreadable
artifact, a media tree that disagrees with the database, and a failed restart
after a freeze. The error message names the artifact or the object.

## Guarantees and limits

- Every artifact is written with a streaming sha256, so nothing large is buffered
  in memory.
- Secrets reach child processes through the environment, never through `argv`.
- Restores refuse to overwrite existing files without `-force`, and refuse archive
  entries that would escape the destination directory.
- Restored files lose setuid, setgid and sticky bits.
- A dump from PostgreSQL *N* restores into *N* or newer; the target version is
  checked before the restore starts.
- Bundles contain production secrets: `.env`, the server RSA key and the Telegram
  Login signing keys. Keep them out of version control.
