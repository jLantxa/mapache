# Mapache User Manual

> **mapache** — a fast, secure, deduplicating, incremental backup tool.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Installation](#2-installation)
3. [Quick Start](#3-quick-start)
4. [Concepts](#4-concepts)
5. [Configuration](#5-configuration)
6. [Hooks](#6-hooks)
7. [Repository Management](#7-repository-management)
8. [Taking Snapshots](#8-taking-snapshots)
9. [Restoring Data](#9-restoring-data)
10. [Exploring Snapshots](#10-exploring-snapshots)
11. [Retention Policies](#11-retention-policies)
12. [Maintenance](#12-maintenance)
13. [Security & Key Management](#13-security--key-management)
14. [Bundle Files](#14-bundle-files)
15. [FUSE Mount](#15-fuse-mount)
16. [Snapshot Lifecycle](#16-snapshot-lifecycle)
17. [Terminal User Interface](#17-terminal-user-interface)
18. [Shell Completions](#18-shell-completions)
19. [Environment Variables](#19-environment-variables)
20. [Global Options Reference](#20-global-options-reference)
21. [Full Command Reference](#21-full-command-reference)
22. [Troubleshooting & FAQ](#22-troubleshooting--faq)

---

## 1. Introduction

Mapache (Spanish for "raccoon") is a high-performance backup tool written in
Rust. It uses a **content-addressable repository model**: every file, directory,
and piece of metadata is decomposed into binary objects identified by their
cryptographic hash (BLAKE3). This enables **global deduplication** — if multiple
files or snapshots share the same content, even across different machines, the
data is stored only once.

Key properties:

- **Deduplication** — FastCDC (Content-Defined Chunking) splits files into
  variable-size chunks; only new chunks are stored.
- **Encryption** — Mandatory AES-GCM-SIV authenticated encryption. Data is
  never stored or transmitted in cleartext.
- **Compression** — Zstd compression with adjustable levels.
- **Snapshots** — Every backup is a point-in-time snapshot. Snapshots are
  logically independent (no "full vs. incremental" chain) but share underlying
  data blobs.
- **Portable** — Single statically-linked binary with zero runtime dependencies.
- **Backends** — Local filesystem, SFTP, and S3-compatible object storage.
- **Verifiable** — Built-in integrity verification at every level.

### Project Status

Mapache is feature-complete and production-oriented, but relatively new. Users
should validate their own backup/restore workflows before relying on it for
mission-critical data.

---

## 2. Installation

### Quick Install (Linux, macOS, Windows)

```bash
curl -fsSL https://github.com/jLantxa/mapache/raw/main/tools/install.sh | sh
```

Downloads the latest release binary for your platform and installs it to
`/usr/local/bin/mapache`. Set `VERSION` or `INSTALL_DIR` to customize.

### Pre-built Binaries

Download binaries from the
[Releases page](https://github.com/jlantxa/mapache/releases).
All binaries are statically linked and require no dependencies.

### Build from Source

Requires the [Rust toolchain](https://rustup.rs/).

```bash
# Build release binary
cargo build --release

# Install to cargo bin path
cargo install --path core
```

For a statically-linked build:

```bash
make release-static
```

### Feature Flags

- `fuse` (default on Linux) — FUSE mount support. Requires `libfuse` headers at
  build time.


To build without FUSE:

```bash
cargo build --release --no-default-features
```

To build with all features:

```bash
cargo build --release --all-features
```

### Docker

A multi-stage Dockerfile is included in the repository root.

---

## 3. Quick Start

### Initialize a Repository

```bash
# Local directory
mapache init -r /backup/myrepo

# SFTP server
mapache init -r sftp://user@host/backup

# S3 bucket
mapache init -r s3://my-bucket/backups
```

You will be prompted for a username and password. **Do not lose this password**
— it is the only way to access your data.

### Create a Snapshot

```bash
mapache snapshot ~/Documents -r /backup/myrepo

# Or pipe data directly via --stdin
tar czf - ~/Documents | mapache snapshot --stdin -r /backup/myrepo
```

### List Snapshots

```bash
mapache log -c -r /backup/myrepo
```

### Restore a Snapshot

```bash
# Restore the latest snapshot
mapache restore --target /tmp/restore -r /backup/myrepo
```

### Apply Retention and Clean Up

```bash
# Keep last 7 daily snapshots, then garbage-collect
mapache forget --keep-daily 7 --clean -r /backup/myrepo
```

---

## 4. Concepts

### Repository

A repository is a directory (local or remote) that stores all backup data. It
contains objects, pack files, indices, keys, snapshots, and locks.

```
repo/
├── index/          # Pack index files (blob → pack mapping)
├── keys/           # User key files
├── locks/          # Runtime lock files
├── manifest        # Repository metadata (ID, version, creation time)
├── objects/        # Pack files (00/..ff/ fanout)
└── snapshots/      # Snapshot metadata files
```

### Content-Addressable Storage

Every blob (chunk of data or tree metadata) is identified by the BLAKE3 hash
of its raw content. This means:

- Identical content produces the same hash → deduplication.
- Any change in content produces a different hash → integrity verification.

### Pack Files

Blobs are not stored individually. They are grouped into **pack files**
(default 16 MiB each) for I/O efficiency. A pack file contains multiple encoded
(compressed + encrypted) blobs and a footer that describes their offsets and
sizes.

### Index

The index maps blob IDs to their physical location (pack ID + offset + length).
It is the source of truth — any blob not in the index is considered non-existent
and subject to garbage collection.

### Snapshots

A snapshot is a point-in-time record of your files. It contains:

- The root tree ID (linking to all file/directory metadata)
- Timestamp, hostname, username
- Paths, tags, description
- Summary statistics (processed bytes, dedup counts, diff counts)

Snapshots are logically independent. Deleting an old snapshot does not affect
newer ones (shared blobs are reference-counted by the index).

### Chunking

Mapache uses FastCDC (Content-Defined Chunking) with:

- Minimum chunk: 512 KiB
- Average chunk: 1 MiB
- Maximum chunk: 8 MiB
- Normalization: L2

Chunker parameters are stable across versions to ensure consistent chunk
boundaries.

### Encryption

All data (packs, indices, snapshots, manifest) is encrypted with AES-GCM-SIV
using a randomly-generated 256-bit master key. User passwords are derived via
Argon2id to produce wrapping keys that encrypt the master key.

---

## 5. Configuration

### Environment Variables

| Variable | Description |
|---|---|
| `MAPACHE_REPOSITORY` | Default repository URL/Path |
| `MAPACHE_USERNAME` | Repository username |
| `MAPACHE_PASSWORD` | Repository password |

### TOML Config File

Mapache can read settings from a TOML configuration file specified via
`--with-config <PATH>`. The file supports `[global]`, `[snapshot]`, `[restore]`,
`[forget]`, `[runtime]`, and `[hooks]` sections.

Generate the canonical template with:

```bash
mapache config template                             # Print to stdout
mapache config template --output ./mapache.toml     # Write to file
```

All values are commented out; uncomment and adjust as needed.

Example configuration:

```toml
[global]
repo = "s3://my-bucket/backups"
no-cache = false
ssh-privatekey = "~/.ssh/id_ed25519"
ssh-known-hosts = "~/.ssh/known_hosts"
auth-file = "~/.mapache/auth"
pack-size-mib = 16.0
key-file = "~/.mapache/repo.key"
quiet = false
compression-level = "fast"
retry-lock = "5m"
limit-upload = "10 MiB/s"
limit-download = "50 MiB/s"

[snapshot]
paths = ["/home/user/Documents"]
exclude = ["**/node_modules", "**/.git"]
tags-str = "work,important"
skip-if-unchanged = false
num-readers = 4
num-packers = 4

[restore]
strategy = "newer"
sparse = true
verify = true

[forget]
keep-daily = 7
keep-weekly = 4
keep-monthly = 6
keep-yearly = 2
keep-min = 3

[runtime]
restore-pack-prefetch = 4
restore-blob-concurrency = 8
restore-max-open-files = 128
min-pack-size-factor = 0.05
gc-repack-concurrency = 2
blobs-per-index-file = 65535
index-flush-timeout-secs = 600
s3-multipart-threshold = 134217728
s3-multipart-part-size = 134217728

[hooks.snapshot.pre]
command = "echo 'Starting snapshot'"

[hooks.snapshot.post]
command = "echo 'Snapshot finished: $MAPACHE_RESULT'"
```

CLI flags override config file values. Config file values supplement CLI lists
(e.g., `exclude` paths from both sources are combined).

### Authentication

Authentication can be provided via:

1. Interactive prompt (default) — prompts for username and password
2. `--auth-file <PATH>` — file with username on the first line and password on
   the second line
3. `MAPACHE_USERNAME` / `MAPACHE_PASSWORD` environment variables
4. `--key-file <PATH>` — path to an external key file (still requires username
   and password for authentication)

When authenticating interactively, mapache allows 3 password attempts before
failing.

---

## 6. Hooks

Mapache supports running custom shell scripts before and after certain commands.
This is useful for notifications, logging, mounting/unmounting filesystems,
or any pre/post processing.

### Configuration

Hooks are defined in the TOML config file under `[hooks.<command>]` sections:

```toml
[hooks.snapshot.pre]
command = "echo 'Starting snapshot'"
timeout = 300

[hooks.snapshot.post]
command = "/usr/local/bin/notify.sh"
```

Each hook can specify:

| Field | Type | Description |
|---|---|---|
| `command` | string | Shell command to execute (required) |
| `timeout` | integer | Max runtime in seconds (optional; no timeout if omitted) |

### Pre and Post Hooks

- **Pre hooks** run before the command starts. If the hook exits with a non-zero
  status, the command is **aborted** immediately.
- **Post hooks** run after the command finishes, regardless of success or
  failure. Non-zero exit status only logs a warning — the command is not
  affected.

### Supported Commands

Hooks can be configured for: `snapshot`, `restore`, `forget`, `clean`, and
`verify`.

### CLI Override

The `--pre-hook` and `--post-hook` flags can be passed on the command line to
override the TOML-configured hook for a single invocation. An empty string
falls through to the TOML hook. When a CLI override is used, the `timeout`
from the TOML config is **not** applied.

```bash
mapache snapshot /data --pre-hook "echo starting"
mapache snapshot /data --post-hook ""   # falls through to TOML hook
```

### Environment Variables

The following variables are passed to every hook script:

| Variable | Description |
|---|---|
| `MAPACHE_COMMAND` | The mapache command being run (e.g. `snapshot`, `restore`) |
| `MAPACHE_REPOSITORY` | The repository URL |
| `MAPACHE_RESULT` | (Post hooks only) `"success"` or the error message if the command failed |

For security, the `MAPACHE_USERNAME` and `MAPACHE_PASSWORD` environment
variables are **stripped** from the hook environment and are never accessible
to hook scripts.

### Timeout

When a timeout is configured and the hook exceeds it, the child process is
killed immediately. For **pre hooks**, this is treated as a failure and the
command is aborted. For **post hooks**, the timeout is only logged as a warning
and does not affect the command result.

### Shell Execution

Hook commands are executed via `sh -c` on Unix and `cmd /C` on Windows.

### Dry Run

When a command is run with `--dry-run`, hooks are **not executed**.

### Example

```toml
[hooks.snapshot.pre]
command = "df -h /backup > /tmp/disk-space-before.txt"

[hooks.snapshot.post]
command = """
  if [ "$MAPACHE_RESULT" = "success" ]; then
    curl -s -X POST https://hooks.example.com/backup-ok \
      -d "repo=$MAPACHE_REPOSITORY&cmd=$MAPACHE_COMMAND"
  fi
"""

[hooks.clean.pre]
command = "/usr/local/bin/maintenance-start.sh"
timeout = 60

[hooks.clean.post]
command = "/usr/local/bin/maintenance-end.sh"
```

---

## 7. Repository Management

### `init` — Initialize a Repository

```bash
mapache init -r <URL>
```

Creates a new empty repository at the given location. Supported URL schemes:

- Local: `/path/to/repo` or `local:/path/to/repo`
- SFTP: `sftp://user@host:port/path`
- S3: `s3://bucket-name/path`

> **SSH key support:** mapache supports Ed25519 and ECDSA keys for SFTP
> authentication. RSA keys are **not** supported — use `ssh-keygen -t ed25519`
> to generate a compatible key pair.

You will be prompted for a username and password. A manifest file is created
with a unique repository ID and the current mapache version. **Do not lose
your password** — there is no password recovery mechanism.

---

### `unlock` — Remove Stale Locks

```bash
mapache unlock -r <URL>
mapache unlock --force -r <URL>
```

Locks are created when mapache opens a repository and automatically released
on clean shutdown. If a process crashes, stale locks may remain. By default,
`unlock` removes only expired locks. Use `--force` to remove all locks
(including non-expired ones).

---

### `cache` — Manage Local Cache

```bash
mapache cache list
mapache cache --delete <PREFIX> [<PREFIX>...]
mapache cache --clear
```

Mapache caches metadata (tree blobs, snapshots, indices) locally to speed up
subsequent operations on remote backends. The `cache` command lists cache
directories by repository ID, deletes specific caches by prefix, or clears all
caches.

---

### `copy` — Transfer Snapshots Between Repositories

```bash
mapache copy --from <URL> -r <URL>
mapache copy --from <URL> --dry-run -r <URL>
mapache copy --from <URL> --snapshot ID --host HOST -r <URL>
```

Copies snapshots (and their data) from one repository to another. Useful for
migration, creating replicas, or consolidating backups.

| Flag | Description |
|---|---|
| `--from <URL>` | Source repository URL (required) |
| `--from-ssh-privatekey <PATH>` | SSH private key for the source repository |
| `--from-ssh-known-hosts <PATH>` | SSH known hosts file for the source repository |
| `--snapshot <PREFIX>` | Copy only snapshots with the given ID prefix (repeatable, or comma-separated) |
| `--host <HOST>` | Copy only snapshots from the given host (repeatable, or comma-separated) |
| `--tags <TAGS>` | Copy only snapshots with the given tag (repeatable, or comma-separated) |
| `--dry-run` | Show what would be copied without transferring data |

The copy operation:

1. Connects to both source (`--from`) and destination (`-r`) repositories
2. Lists all snapshots in the source, applying any filters (`--snapshot`, `--host`, `--tags`)
3. Skips snapshots already present in the destination (by ID)
4. Walks every referenced tree to discover all required data blobs
5. Filters out blobs already present in the destination (blob-level dedup)
6. Transfers only missing blobs, re-packing them at the destination
7. Writes snapshot metadata to the destination

Supports `--json` for machine-readable progress events.

---

### `sync` — Replicate a Repository

```bash
mapache sync --target <URL> -r <URL>
mapache sync --target <URL> --delete -r <URL>
mapache sync --target <URL> --dry-run -r <URL>
```

Synchronizes a repository to a different backend. Useful for creating replicas
or migrating between storage providers.

| Flag | Description |
|---|---|
| `--target <URL>` | Destination repository URL (required) |
| `--delete` | Delete files in destination that do not exist in source |
| `--dst-ssh-privatekey <PATH>` | SSH private key for destination |
| `--dry-run` | Show what would be copied/deleted without making changes |

The sync operation:

1. diffs source vs destination
2. deletes obsolete files (destination-only)
3. copies missing or changed files
4. synchronizes the manifest last to ensure destination validity.

---

### `rebuild-index` — Disaster Recovery

```bash
mapache rebuild-index
mapache rebuild-index --dry-run
```

Rebuilds the master index from scratch by scanning all existing pack files and
parsing their footers. Used for disaster recovery when the index is lost or
corrupted but all pack files are intact.

The command will:

1. List all pack files in the repository
2. Parse each pack footer to discover blobs
3. Build a new master index
4. Persist the new index
5. Delete old/replaced index files

---

## 8. Taking Snapshots

### Basic Usage

```bash
mapache snapshot PATH [PATH...] -r <URL>
```

One or more paths can be specified. Mapache backs up each path, deduplicating
shared content across all of them.

### Common Options

| Flag | Description |
|---|---|
| `--as-root` | Use a single directory as the snapshot root (its children become top-level items) |
| `--exclude PATH[,PATH,...]` | Comma-separated paths to exclude (repeatable) |
| `--exclude-file <PATH>` | File with exclude paths, one per line |
| `--tags <TAGS>` | Comma-separated tags for the snapshot |
| `--description <TEXT>` | Snapshot description |
| `--no-parent` | Force complete analysis of all files (skip parent diff) |
| `--no-scan` | Skip the initial file system scan |
| `--skip-if-unchanged` | Do not save a snapshot if no changes detected |
| `--parent <ID\|"latest">` | Use a specific snapshot as the parent for diffing |
| `--readers <N>` | Number of parallel file readers (default: 4) |
| `--packers <N>` | Number of parallel writer/packer threads (default: 4) |
| `--dry-run` | Simulate the backup without storing any data |
| `--stdin` | Read backup data from stdin as a single file at `/stdin`. Mutually exclusive with paths, excludes, and parent snapshot |
| `--with-atime` | Store file access times (off by default; may increase metadata size) |

### Exclusions

Exclusions support glob patterns (`**`, `*`, `?`) as well as literal paths.
Non-glob paths are normalized to absolute paths; glob patterns are kept as-is
for matching. They can be specified inline or in a file:

```bash
# Inline exclusions
mapache snapshot ~/project --exclude "~/project/target","~/project/.git"

# Exclusion file (one path per line)
mapache snapshot ~/project --exclude-file ~/.mapache-exclude
```

### Tags

Tags are comma-separated labels used for filtering snapshots in `log`,
`forget`, and other commands.

```bash
mapache snapshot ~/Documents --tags "work,important"
```

### Incremental Backups

By default, mapache uses the latest snapshot as the parent for incremental
diffing. Only changed/new files are processed; unchanged files are skipped.

```bash
# Explicitly set the parent snapshot
mapache snapshot ~/Documents --parent latest

# Force a full scan (ignore parent)
mapache snapshot ~/Documents --no-parent

# Use a specific snapshot as parent
mapache snapshot ~/Documents --parent abc1234f
```

### Skip If Unchanged

When `--skip-if-unchanged` is set, mapache compares the new snapshot's root
tree with the parent's. If they match, no new snapshot file is created.

### Parallelism

- `--readers`: Controls how many files are read and chunked concurrently.
- `--packers`: Controls how many threads compress, encrypt, and write pack
  files concurrently.

Higher values improve throughput on fast storage but consume more memory.

### Dry Run

Using `--dry-run` shows what would be backed up without writing any data:

```bash
mapache snapshot ~/Documents --dry-run -r /backup/myrepo
```

Hooks are also skipped during dry runs (see [Hooks](#6-hooks)).

### Stdin Backup

Use `--stdin` to back up data from a pipe or redirected file as a single
virtual file at `/stdin`:

```bash
# Backup a database dump
pg_dump mydb | mapache snapshot --stdin -r /backup/myrepo

# Backup a tar archive
tar czf - /important/data | mapache snapshot --stdin -r /backup/myrepo --tags "archive"

# Redirect a file
mapache snapshot --stdin -r /backup/myrepo < /path/to/largefile.bin
```

The data is chunked, deduplicated, compressed, and encrypted identically to
regular files. The `--stdin` flag is mutually exclusive with paths, excludes,
and `--parent`.

> **Important**: `--stdin` requires non-interactive authentication (stdin is
> consumed by backup data, so the password cannot be prompted interactively).
> Use either `--auth-file` or `MAPACHE_USERNAME`/`MAPACHE_PASSWORD` env vars:
>
> ```bash
> # Option 1: auth file
> echo "myusername" > ~/.mapache-auth
> echo "mypassword" >> ~/.mapache-auth
> mapache snapshot --stdin -r /backup/myrepo --auth-file ~/.mapache-auth
>
> # Option 2: environment variables
> MAPACHE_USERNAME=myusername MAPACHE_PASSWORD=mypassword \
>   mapache snapshot --stdin -r /backup/myrepo
> ```

### Snapshot Output

After a successful snapshot, mapache displays:

- Changes since parent (new/changed/deleted/unchanged files and dirs)
- Data and metadata sizes (raw vs compressed)
- The new snapshot ID

---

## 9. Restoring Data

### Basic Usage

```bash
mapache restore [SNAPSHOT] --target <DIR> -r <URL>
```

Restores a snapshot to the given target directory. The `SNAPSHOT` argument can
be a snapshot ID prefix or `latest` (default).

### Options

| Flag | Description |
|---|---|
| `--target <PATH>` | Restore destination (required) |
| `--include PATH[,PATH,...]` | Only restore these paths (repeatable, or comma-separated within a single flag) |
| `--include-file <PATH>` | File with paths to include, one per line |
| `--exclude PATH[,PATH,...]` | Exclude these paths (repeatable, or comma-separated within a single flag) |
| `--exclude-file <PATH>` | File with paths to exclude, one per line |
| `--strip-prefix` | Strip the longest common prefix from restored paths |
| `--strategy <MODE>` | Conflict resolution: `fail` (default), `skip`, `overwrite`, `newer` |
| `--delete` | Delete files in target that are not in the snapshot |
| `--no-preserve-root` | Allow `--delete` to remove root-level items |
| `--quit-on-error` | Exit immediately on the first restore error |
| `--sparse` | Create sparse files instead of preallocating |
| `--verify` | Verify content by hashing each blob and skip unchanged data (incremental restore) |
| `--batch-size <N>` | Max files per batch. Limits peak memory. Default: no limit (all files at once) |
| `--dry-run` | Simulate restoration |

### Conflict Strategies

| Strategy | Behavior |
|---|---|
| `fail` (default) | Abort with an error if any file already exists |
| `skip` | Keep the existing file, skip restoring |
| `overwrite` | Replace the existing file unconditionally |
| `newer` | Keep the file with the more recent modification time |

### Path Filtering

Restore supports both include and exclude filtering:

```bash
# Restore only specific paths
mapache restore --target /tmp/restore --include "docs/,src/main.rs"

# Exclude certain paths
mapache restore --target /tmp/restore --exclude "*.log,*.tmp"

# Use filter files
mapache restore --target /tmp/restore --include-file restore-list.txt
```

### Strip Prefix

When restoring a subset of paths, `--strip-prefix` removes the common ancestor
directory:

```bash
# Snapshot contains: /home/user/project/src/main.rs
# Without --strip-prefix: /tmp/restore/home/user/project/src/main.rs
# With --strip-prefix:    /tmp/restore/src/main.rs
```

### Delete Mode

`--delete` removes files in the target directory that do not exist in the
snapshot. Use with caution. `--no-preserve-root` extends deletion to the
root level of the restored tree.

### Sparse Files

`--sparse` creates sparse files instead of preallocating space. This is faster
on some filesystems but may cause fragmentation.

### Verification

By default, restore decides whether to overwrite a file based on the chosen
strategy alone. Use `--verify` to enable **content-level verification**:
the restorer reads each existing local file, hashes every blob, and compares
the result against the repository index.

| Strategy | verify=off | verify=on |
|---|---|---|
| `overwrite` | Always rewrites | Reads local file, skips unchanged blobs, only downloads and writes changed ones |
| `skip` | Always skips | Always skips |
| `fail` | Error if file exists | Error if file exists |
| `newer` | Compares modification times | Reads local file and compares blob-by-blob; falls back to mtime if sizes differ or verification fails |

When `--verify` is active with `overwrite` or `newer`, the restorer performs
**incremental restore**: each blob is individually compared against the local
file content. Only blobs whose hash differs from the on-disk data are
downloaded and written; unchanged blobs are skipped. This significantly speeds
up repeated restores of large files where only a small fraction of the content
changed.

If verification fails (e.g. unreadable file), the restorer falls back to the
strategy's default behavior.

### Dry Run

```bash
mapache restore --target /tmp/restore --dry-run
```

Simulates the full restore process (including conflict detection, path
filtering, and deletion) without touching the filesystem. Hooks are also
skipped (see [Hooks](#6-hooks)).

### Restore Output

Displays progress, a summary of errors and warnings, and total duration.

---

## 10. Exploring Snapshots

### `log` — List Snapshots

```bash
mapache log -r <URL>
mapache log -c -r <URL>           # Compact listing
mapache log --all -r <URL>        # Show active + dropped
mapache log --dropped -r <URL>    # Show only dropped
mapache log --tags "work" -r <URL> # Filter by tags
mapache log SNAPSHOT_ID -r <URL>  # Show specific snapshot
```

Full listing shows: ID, date, size, root, hostname, username, tags, paths,
and description. Compact mode (`-c`) shows a summary table.

---

### `ls` — Browse Snapshot Contents

```bash
mapache ls -r <URL>                        # List root of latest snapshot
mapache ls SNAPSHOT_ID -r <URL>            # List root of specific snapshot
mapache ls --path /home/user/docs -r <URL> # List specific path
mapache ls -l -r <URL>                     # Long listing (permissions, size, mtime)
mapache ls -H -r <URL>                     # Human-readable sizes
mapache ls -R -r <URL>                     # Recursive listing
```

---

### `find` — Search Snapshots

```bash
mapache find PATTERN -r <URL>              # Search in all snapshots
mapache find PATTERN --snapshot latest -r <URL>  # Search in latest only
mapache find PATTERN --snapshot ID -r <URL>      # Search in specific snapshot
mapache find PATTERN --json -r <URL>       # JSON output
```

Searches file and directory names by pattern across one or all snapshots.
When the pattern does not contain a `/`, the search is recursive and matches
in any subdirectory. Use a leading `/` (e.g. `find /file.txt`) to restrict
the search to the root directory of each snapshot.

Supports `--json` for machine-readable output. Exits with a non-zero code on
error.

---

### `diff` — Compare Snapshots

```bash
mapache diff SOURCE_ID TARGET_ID -r <URL>
```

Shows the differences between two snapshots. Symbol legend:

| Symbol | Meaning |
|---|---|
| `+` (green) | New file/directory |
| `-` (red) | Deleted file/directory |
| `M` (yellow) | Modified content |
| `m` (cyan) | Metadata changed |
| `T` (purple) | Type changed (file ↔ dir) |
| `U` (white) | Unchanged |

Output includes a summary table and a size comparison.

---

### `stats` — Repository Statistics

```bash
mapache stats -r <URL>
mapache stats --full -r <URL>   # Parse pack footers (expensive)
```

Shows:

- **Packs**: count, total size, optional footer analysis (blob count, encoded
  vs raw sizes, dangling blobs)
- **Index**: count, size, indexed blobs, raw/encoded sizes
- **Snapshots**: count, total snapshot metadata size
- **Snapshot reference summary**: referenced blobs (data + tree), raw/encoded
  sizes, compression ratios, total restorable size
- **Keys**: count, total size
- **Manifest**: size
- **Total repository size**

`--full` parses every pack footer, giving physical blob statistics and
dangling blob detection (expensive on large repositories).

---

### `dump` — Print File Contents

```bash
mapache dump SNAPSHOT_ID --path /path/to/file -r <URL>
mapache dump latest --path /path/to/file -r <URL>
```

Prints the contents of a single file from a snapshot to stdout. The
`SNAPSHOT_ID` argument accepts ID prefixes or `latest`.

---

### `cat` — Print Raw Repository Objects

```bash
mapache cat manifest -r <URL>
mapache cat snapshot:ID -r <URL>
mapache cat pack:ID -r <URL>
mapache cat blob:ID -r <URL>
mapache cat tree:ID -r <URL>
mapache cat index:ID -r <URL>
mapache cat key:ID -r <URL>
mapache cat lock:ID -r <URL>
```

Prints the raw JSON content of repository objects. Useful for debugging and
manual inspection.

---

## 11. Retention Policies

### `forget` — Remove Snapshots

```bash
mapache forget [SNAPSHOT_IDS...] -r <URL>     # Remove specific snapshots
mapache forget --keep-daily 7 -r <URL>         # Apply retention rules
mapache forget --keep-last 5 --clean -r <URL>  # Keep + GC
mapache forget --force SNAPSHOT_ID -r <URL>    # Permanently delete
```

### Retention Rules

By default, `forget` marks snapshots as dropped (renames to `.dropped`).
Use `--force` for immediate deletion.

| Rule | Description |
|---|---|
| `--keep-last <N\|all>` | Keep the last N snapshots by time. `all` keeps every snapshot. `0` and negatives are rejected. |
| `--keep-within <DUR>` | Keep all snapshots within a duration (e.g. `1d`, `2w`, `3m`, `30d`) |
| `--keep-hourly <N\|all>` | Keep N hourly snapshots. `all` keeps the latest snapshot in every hour that has one. |
| `--keep-daily <N\|all>` | Keep N daily snapshots. `all` keeps the latest snapshot in every day that has one. |
| `--keep-weekly <N\|all>` | Keep N weekly snapshots. `all` keeps the latest snapshot in every week that has one. |
| `--keep-monthly <N\|all>` | Keep N monthly snapshots. `all` keeps the latest snapshot in every month that has one. |
| `--keep-yearly <N\|all>` | Keep N yearly snapshots. `all` keeps the latest snapshot in every year that has one. |
| `--keep-tags <TAGS>` | Always keep snapshots with these tags |
| `--keep-min <N\|all>` | Always keep at least N snapshots after applying rules |

A snapshot is kept if **any** rule applies. Rules are inclusive, not exclusive.

### Filtering

```bash
# Only consider snapshots with specific tags
mapache forget --keep-daily 7 --tags "work"

# Only consider snapshots from specific hosts
mapache forget --keep-daily 7 --host my-server
```

### Garbage Collection Integration

```bash
# Run GC after forget
mapache forget --keep-daily 7 --clean

# Custom GC tolerance
mapache forget --keep-daily 7 --clean --tolerance 10
```

### Dry Run

```bash
mapache forget --keep-daily 7 --dry-run
```

Shows which snapshots would be kept and removed without making changes.
Hooks are also skipped during dry runs (see [Hooks](#6-hooks)).

---

## 12. Maintenance

### `clean` — Garbage Collection

```bash
mapache clean -r <URL>               # Full GC with 0% tolerance
mapache clean --tolerance 10 -r <URL> # Tolerate 10% garbage per pack
mapache clean --no-repack -r <URL>    # Only remove unreferenced packs
mapache clean --dry-run -r <URL>      # Simulate
```

The garbage collector:

1. Scans all snapshots to determine which blobs are still referenced.
2. Identifies:
   - **Unused packs** — packs where no blobs are referenced (removed).
   - **Obsolete packs** — packs with blobs that will be repacked.
   - **Small packs** — packs smaller than the minimum size threshold (repacked).
   - **Tolerated packs** — packs with garbage below the tolerance threshold
     (kept as-is).
3. Repacks small packs and obsolete-pack survivors into new pack files.
4. Removes orphaned index entries.

`--no-repack` skips repacking (equivalent to tolerance = 100%).

The garbage collector can be safely interrupted with SIGINT/SIGTERM (Ctrl+C).
The shutdown signal is checked at safe checkpoints between GC phases, so no
data corruption occurs. This also applies to `forget --clean`.

Hooks are skipped when running with `--dry-run` (see [Hooks](#6-hooks)).

---

### `verify` — Integrity Verification

```bash
mapache verify -r <URL>                    # Logical consistency check
mapache verify --read-packs -r <URL>       # Full physical verification
mapache verify --read-packs --sample 10%   # Verify 10% of packs
mapache verify --read-packs --parallel 8   # Parallel verification
mapache verify --fail-early                # Stop on first error
mapache verify --with-cache                # Use local cache
```

#### Logical Verification (default)

- Checks that every snapshot references known tree blobs.
- Checks that every indexed blob points to an existing pack file.
- Verifies snapshot tree traversal integrity.

#### Physical Verification (`--read-packs`)

- Reads, decrypts, and hashes every blob in every pack (or a sample).
- Detects bit-rot (pack file hash mismatch) and individual blob corruption.
- Reports corrupt blobs and the files/snapshots they affect.

#### Sampling

`--sample <PCT>` verifies only a random percentage of packs. Useful for
periodic integrity checks on large repositories.

---

### `rechunk` — Re-process All Snapshots

```bash
mapache rechunk -r <URL>
```

Re-processes all snapshots using the current chunker parameters. Useful after
chunker algorithm changes or when migrating between versions. Creates new blob
versions and updates snapshot trees. Old blobs remain until cleaned by GC.

Requires an exclusive lock. May take significant time on large repositories.

---

## 13. Security & Key Management

Mapache uses a dual-layer encryption model:

1. **Master Key** — A random 256-bit AES-GCM-SIV key stored encrypted inside
   key files. This key encrypts all repository data.
2. **User Keys** — Derived from username + password via Argon2id KDF. Each user
   key encrypts the master key. Multiple users can access the same repository.

Key files can be stored inside the repository (`keys/` directory) or exported
as standalone files.

### `key list` — List Keys

```bash
mapache key list -r <URL>
```

Shows all key files in the repository: username, key ID, creation date.

### `key add` — Add a New User

```bash
mapache key add -r <URL>                     # Store key in repository
mapache key add --path ~/my-backup-key -r <URL>  # Export key file locally
```

Requires an existing user's credentials to decrypt the master key, then
prompts for the new user's username and password.

### `key delete` — Remove a User

```bash
mapache key delete KEY_ID -r <URL>
```

Deletes a key file by ID prefix. The affected user will no longer be able to
access the repository.

### `key change-password` — Change Password

```bash
mapache key change-password -r <URL>
```

Prompts for current credentials, then for a new password. Generates a new key
file and deletes the old one.

### `key export` — Export a Key File

```bash
mapache key export KEY_ID -r <URL>           # Exports to ./keyfile
mapache key export KEY_ID --output ~/safe/keyfile -r <URL>
```

Exports a key file from the repository to a local path. This file can be used
later with `--key-file` to authenticate without username/password.

### Security Best Practices

- **Never lose your password** — there is no recovery mechanism.
- Export key files to a secure location for disaster recovery.
- Use `--key-file` to authenticate without typing passwords (useful for scripts).
- The repository is a **black box** — all data is encrypted. An attacker with
  full read access to the storage cannot decrypt any content without the master
  key.

---

## 14. Bundle Files

The `bundle` command creates, extracts, imports, exports, or mounts self-contained
`.mapache` bundle files. Bundles are encrypted, deduplicated archives that can be
transferred to other systems.

### Create a Bundle

```bash
# Create a bundle from files/directories
mapache bundle -a PATH [PATH...] -o bundle.mapache

# With exclusions
mapache bundle -a ~/project -o project.mapache -e "*.log" -e "tmp/"

# Custom compression
mapache bundle -a ~/Documents -o docs.mapache --compression best
```

### Extract a Bundle

```bash
mapache bundle -x bundle.mapache -o /tmp/extract
```

If `-o` is omitted, extracts to the current directory.

### Export a Snapshot to a Bundle

```bash
# Export the latest snapshot
mapache bundle --export-snapshot latest -o backup.mapache -r /path/to/repo

# Export by snapshot ID prefix
mapache bundle --export-snapshot a1b2c3d4 -o backup.mapache -r /path/to/repo
```

The exported bundle contains all data from the snapshot. The bundle's blob IDs
match the repository, so importing the bundle into the same (or another)
repository will deduplicate automatically against existing blobs.

### Import a Bundle as a Snapshot

```bash
# Import a bundle as a snapshot into the repository
mapache bundle -i bundle.mapache -r /path/to/repo

# Import multiple bundles at once (each becomes a separate snapshot)
mapache bundle -i bundle1.mapache bundle2.mapache -r /path/to/repo
```

Blobs already present in the repository are skipped, making this efficient for
cross-repo transfers. Each imported snapshot appears in `log` with a description
recording which bundle file it came from.

### Mount a Bundle (FUSE, Unix only)

```bash
mapache bundle -m bundle.mapache /mnt/bundle
mapache bundle -m bundle.mapache /mnt/bundle -c      # Auto-create mountpoint
mapache bundle -m bundle.mapache /mnt/bundle --metadata-only
```

Bundle-specific options:

| Flag | Description |
|---|---|
| `--export-snapshot <ID>` | Export mode: export snapshot to a bundle (requires `-r`) |
| `-i, --import` | Import mode: import bundle as snapshot (requires `-r`) |
| `-e, --exclude <GLOB>` | Glob patterns to exclude (bundle mode only) |
| `--readers <N>` | Parallel readers (default: 4) |
| `-c, --create` | Create mountpoint if it does not exist (mount mode) |
| `--allow-other` | Allow other users to access the mount |
| `--metadata-only` | Display file listing without loading contents |
| `--cache-size-mib <MIB>` | Data cache size (default: 256 MiB, mount mode) |

Bundles are password-protected (AES-GCM-SIV). The password is independent
from the repository password.

---

## 15. FUSE Mount

### Mount a Repository (Unix only, `fuse` feature)

```bash
mapache mount /mnt/backup -r <URL>
mapache mount /mnt/backup -r <URL> --allow-other
mapache mount /mnt/backup -r <URL> --metadata-only
mapache mount /mnt/backup -r <URL> --create-mountpoint
mapache mount /mnt/backup -r <URL> --cache-size-mib 128
```

Mounts the entire repository as a FUSE filesystem. All snapshots are listed at
the root; each snapshot directory contains the backed-up files.

### Mount a Bundle

```bash
mapache mount bundle.mapache /mnt/bundle
mapache mount bundle.mapache /mnt/bundle -b    # Force bundle mode
```

If the path has a `.mapache` extension, mapache auto-detects it as a bundle.

### Mount Options

| Flag | Description |
|---|---|
| `--allow-other` | Allow other users to access the mount |
| `-c, --create-mountpoint` | Create the mountpoint if it does not exist |
| `--metadata-only` | List files without loading their contents |
| `--cache-size-mib <MIB>` | Internal data cache size (default: 64 MiB for repos, 256 MiB for bundles) |

Press `Ctrl+C` to unmount.

---

## 16. Snapshot Lifecycle

### `amend` — Modify a Snapshot

```bash
# Modify tags
mapache amend --tags "updated,important" -r <URL>

# Modify description
mapache amend --description "Backup before upgrade" -r <URL>

# Clear metadata
mapache amend --clear-tags -r <URL>
mapache amend --clear-description -r <URL>

# Add exclusions post-hoc (rebuilds tree)
mapache amend --exclude "*.log" -r <URL>

# Apply to all snapshots
mapache amend --all --tags "archived" -r <URL>

# Keep the old snapshot file
mapache amend --keep-old -r <URL>
```

When exclusions are added, mapache rebuilds the snapshot tree (creating new
tree blobs) without re-chunking file contents. The old snapshot file is
deleted unless `--keep-old` is set.

### `recall` — Recover Dropped Snapshots

```bash
mapache recall SNAPSHOT_ID -r <URL>
```

Restores a snapshot that was previously dropped (marked with `.dropped`
extension) by `forget`. The snapshot is reactivated and will appear in
`log` listings again.

---

## 17. Terminal User Interface

```bash
mapache tui -r <URL>
```

Launches an interactive terminal user interface with screens for:

- **Dashboard** — overview of the repository with version info and key stats
- **File explorer** — browse snapshot contents with inline detail panel
- **Snapshot detail** — inspect individual snapshots
- **Snapshot creation wizard**
- **Restore wizard**
- **Forget/retention management**
- **Diff screen** — navigable tree of changes between snapshots (`<`/`>` to
  browse adjacent pairs, `/` to filter, `u` to toggle unchanged files)
- **Find screen** — real-time glob search across all snapshots with progress,
  results table, and direct navigation to explorer/restore

Requires a supported terminal.

---

## 18. Shell Completions

```bash
mapache completion --shell bash --path /etc/bash_completion.d/
mapache completion --shell zsh --path /usr/share/zsh/site-functions/
mapache completion --shell fish --path ~/.config/fish/completions/
mapache completion --shell powershell --path ~/Documents/PowerShell/
```

Generates shell completion scripts for the given shell. Supported shells
include: `bash`, `zsh`, `fish`, `powershell`, `elvish`, and `nushell`.

---

## 19. Environment Variables

| Variable | Description |
|---|---|
| `MAPACHE_REPOSITORY` | Default repository URL/path (replaces `--repo`/`-r`) |
| `MAPACHE_USERNAME` | Repository username (used for authentication) |
| `MAPACHE_PASSWORD` | Repository password (used for authentication) |
| `MAPACHE_REFRESH_RATE` | UI progress refresh rate in Hz (float) |
| `MAPACHE_DEBUG_PATH` | Directory to write debug logs to |
| `MAPACHE_DEBUG_LEVEL` | Debug log level: `trace`, `debug`, `info`, `warn`, `error` (default: `debug`) |

If `MAPACHE_REPOSITORY` is set, the `-r` flag can be omitted for all commands.

If `MAPACHE_USERNAME` and `MAPACHE_PASSWORD` are set, interactive
authentication prompts are skipped.

---

## 20. Global Options Reference

These options are available on most commands (exceptions: `bundle`, `cache`,
`completion` do not use repository connection).

| Flag | Description |
|---|---|
| `-r, --repo <URL>` | Repository path or URL. Also read from `MAPACHE_REPOSITORY` |
| `--with-config <PATH>` | Path to TOML configuration file |
| `--no-cache` | Disable local caching |
| `--no-lock` | Open repository without acquiring a lock (read-only; skips lock file creation) |
| `--ssh-privatekey <PATH>` | SSH private key file (SFTP backend) |
| `--ssh-known-hosts <PATH>` | SSH known hosts file (SFTP backend) |
| `--auth-file <PATH>` | File with username on the first line and password on the second line |
| `--pack-size <MIB>` | Pack target size in MiB (1–4095, default: 16) |
| `-k, --key-file <PATH>` | Path to an external key file |
| `--quiet` | Suppress all logging output (verbosity = 0) |
| `-v, --verbosity <0-3>` | Verbosity level (0 = quiet, 3 = most verbose) |
| `--compression <LEVEL>` | Compression: `fastest`, `fast` (default), `balanced`, `better`, `best`, or `level:N` (N = 1–19) |
| `--retry-lock <DUR>` | Retry acquiring a lock for a duration (e.g., `5m`, `30s`, `5m30s`) |
| `--limit-upload <SPEED>` | Upload bandwidth limit (e.g., `10MB/s`, `500KB/s`, `1G`) |
| `--limit-download <SPEED>` | Download bandwidth limit (e.g., `10MB/s`, `500KB/s`, `1G`) |
| `--json` | Output results in JSON format (supported by most commands) |
| `--pre-hook <CMD>` | Shell command to run before the main command (overrides TOML config) |
| `--post-hook <CMD>` | Shell command to run after the main command (overrides TOML config) |

### Compression Levels

| Preset | zstd Level | Use Case |
|---|---|---|
| `fastest` | 1 | Maximum speed, minimum CPU |
| `fast` (default) | 3 | Good balance |
| `balanced` | 5 | Balanced speed/ratio |
| `better` | 10 | Better compression, slower |
| `best` | 19 | Maximum compression, slowest |
| `level:N` | N | Custom zstd level (1–19) |

### Bandwidth Limit Format

```
1000        -> 1000 bytes/s
1K / 1KB   -> 1000 bytes/s  (or 1024 for K)
1M / 1MB   -> 1,000,000 bytes/s  (or 1,048,576 for M)
1G / 1GB   -> 1,000,000,000 bytes/s  (or 1,073,741,824 for G)
1.5M       -> 1,572,864 bytes/s
10MiB/s    -> 10,485,760 bytes/s
```

### Duration Format

```
30s     -> 30 seconds
5m      -> 5 minutes
5m30s   -> 5 minutes 30 seconds
1h      -> 1 hour
1d      -> 1 day
2w      -> 2 weeks
3m      -> 3 months (30-day months)
4y      -> 4 years (365-day years)
```

---

## 21. Full Command Reference

### `mapache init`

Initialize a new backup repository.

```
mapache init -r <URL>
```

### `mapache snapshot`

Create a new backup snapshot.

```
mapache snapshot [PATHS...] -r <URL>
  --as-root               Use a single directory as the snapshot root
  --exclude <PATH,...>    Paths to exclude (repeatable, or comma-separated)
  --exclude-file <PATH>   File with exclude paths (one per line)
  --tags <TAGS>           Comma-separated tags
  --description <TEXT>    Snapshot description
  --no-parent             Force complete analysis (no parent diff)
  --no-scan               Skip the initial file system scan
  --skip-if-unchanged     Skip saving if no changes detected
  --parent <ID|latest>    Use specific parent snapshot
  --readers <N>           Parallel file readers (default: 4)
  --packers <N>           Writer threads (default: 4)
  --dry-run               Simulate without making changes
  --stdin                 Read data from stdin as file /stdin
  --with-atime            Store file access times
  --pre-hook <CMD>        Shell command to run before snapshot (overrides config)
  --post-hook <CMD>       Shell command to run after snapshot (overrides config)
```

### `mapache restore`

Restore a snapshot to a target path.

```
mapache restore [SNAPSHOT] --target <PATH> -r <URL>
  --target <PATH>         Restore destination (required)
  --include <PATH,...>    Paths to include (repeatable, or comma-separated)
  --include-file <PATH>   File with include paths (one per line)
  --exclude <PATH,...>    Paths to exclude (repeatable, or comma-separated)
  --exclude-file <PATH>   File with exclude paths (one per line)
  --strip-prefix          Strip longest common prefix from restored paths
  --strategy <MODE>       Conflict resolution: fail|skip|overwrite|newer
  --delete                Delete files in target not in snapshot
  --no-preserve-root      Allow --delete at root level
  --quit-on-error         Exit immediately on restore error
  --sparse                Create sparse files
  --verify                Verify content by hashing each blob (incremental restore)
  --batch-size <N>        Max files per batch (limits memory; default: no limit)
  --pre-hook <CMD>        Shell command to run before restore (overrides config)
  --post-hook <CMD>       Shell command to run after restore (overrides config)
  --dry-run               Simulate restoration
```

### `mapache forget`

Remove snapshots and apply retention policies.

```
mapache forget [SNAPSHOT_IDS...] -r <URL>
  --force                 Immediately delete (don't just mark)
  --tags <TAGS>           Filter by tags
  --host <HOST>           Filter by host (repeatable)
  --keep-last <N|all>     Keep last N snapshots (or all)
  --keep-within <DUR>     Keep snapshots within duration
  --keep-hourly <N|all>   Keep N hourly snapshots
  --keep-daily <N|all>    Keep N daily snapshots
  --keep-weekly <N|all>   Keep N weekly snapshots
  --keep-monthly <N|all>  Keep N monthly snapshots
  --keep-yearly <N|all>   Keep N yearly snapshots
  --keep-tags <TAGS>      Always keep snapshots with these tags
  --keep-min <N|all>      Minimum snapshots to keep
  --dry-run               Show what would be removed
  --clean                 Run garbage collector after
  -t, --tolerance <%>     GC garbage tolerance (0–100)
  --pre-hook <CMD>        Shell command to run before forget (overrides config)
  --post-hook <CMD>       Shell command to run after forget (overrides config)
```

### `mapache log`

Show snapshots in the repository.

```
mapache log [SNAPSHOT_ID] -r <URL>
  --dropped               Show only dropped snapshots
  --all                   Show active and dropped snapshots
  -c, --compact           Compact table listing
  --tags <TAGS>           Filter by tags
```

### `mapache ls`

List nodes/files in a snapshot.

```
mapache ls [SNAPSHOT] -r <URL>
  --path <PATH>           Path within snapshot
  -l, --long              Long listing format
  -H, --human-readable    Human-readable sizes
  -R, --recursive         Recursive listing
```

### `mapache find`

Find files and directories in the repository.

```
mapache find <TARGET> -r <URL>
  --snapshot <ID|latest>  Search in specific snapshot (all if omitted)
  --json                  Output results in JSON format
```

Exits with a non-zero code on error.

### `mapache diff`

Show differences between snapshots.

```
mapache diff <SOURCE_ID> <TARGET_ID> -r <URL>
```

### `mapache dump`

Print the contents of a file from a snapshot to stdout.

```
mapache dump [SNAPSHOT] -r <URL>
  --path <PATH>           Path to the file inside the snapshot (required)
```

### `mapache stats`

Show repository statistics.

```
mapache stats -r <URL>
  --full                  Parse pack footers for physical stats (expensive)
```

### `mapache clean`

Run garbage collection.

```
mapache clean -r <URL>
  -t, --tolerance <%>     Garbage tolerance (0–100, default: 0)
  --no-repack             Skip repacking
  --dry-run               Simulate without making changes
  --pre-hook <CMD>        Shell command to run before clean (overrides config)
  --post-hook <CMD>       Shell command to run after clean (overrides config)
```

### `mapache verify`

Verify repository integrity.

```
mapache verify -r <URL>
  --read-packs            Read, decrypt, hash all data (slow)
  -p, --parallel <N>      Parallel pack reading (default: 4)
  --with-cache            Use local cache (disabled by default)
  --fail-early            Stop on first error
  --sample <%>            Verify random sample of packs (e.g., 10.5%)
  --pre-hook <CMD>        Shell command to run before verify (overrides config)
  --post-hook <CMD>       Shell command to run after verify (overrides config)
```

### `mapache key`

Manage repository key files.

```
mapache key list -r <URL>
mapache key add -r <URL> [--path <PATH>]
mapache key delete <KEY_ID> -r <URL>
mapache key change-password -r <URL>
mapache key export <KEY_ID> -r <URL> [-o <PATH>]
```

### `mapache bundle`

Create, extract, import, export, or mount `.mapache` bundle files.

```
mapache bundle -a <INPUT...> -o <OUTPUT.mapache> [OPTIONS]
mapache bundle -x <INPUT.mapache> [-o <DIR>] [OPTIONS]
mapache bundle --export-snapshot <SNAPSHOT> -o <OUTPUT.mapache> -r <URL>
mapache bundle -i <INPUT.mapache...> -r <URL>
mapache bundle -m <INPUT.mapache> <MOUNTPOINT> [OPTIONS]

  -a, --bundle            Create mode
  -x, --extract           Extract mode
  -i, --import            Import mode: one or more bundle files (requires -r)
  --export-snapshot <SNAPSHOT>  Export mode: export snapshot (latest, prefix, or ID; requires -r)
  -m, --mount             Mount mode (FUSE, Unix only)
  -o, --output <PATH>     Bundle file (-a, --export-snapshot) or destination (-x)
  -e, --exclude <GLOB>    Exclude globs (bundle mode)
  --readers <N>           Parallel readers (default: 4)
  -c, --create            Create mountpoint (mount mode)
  --allow-other           Allow other users (mount mode)
  --metadata-only         Don't load file contents (mount mode)
  --cache-size-mib <MIB>  Data cache size (mount mode, default: 256)
```

### `mapache mount`

Mount repository or bundle as FUSE filesystem (Unix only).

```
mapache mount <MOUNTPOINT> -r <URL>
  -b, --bundle            Force bundle mount
  --allow-other           Allow other users
  -c, --create-mountpoint Create mountpoint if missing
  --metadata-only         Don't load file contents
  --cache-size-mib <MIB>  Data cache size (default: 64)
```

Exits with a non-zero code on error.

### `mapache cat`

Print repository objects.

```
mapache cat manifest -r <URL>
mapache cat <type>:<ID> -r <URL>
  Types: manifest, pack, blob, tree, index, key, snapshot, lock
```

### `mapache amend`

Modify an existing snapshot.

```
mapache amend [SNAPSHOT] -r <URL>
  -a, --all               Apply to all snapshots
  --keep-old              Keep the old snapshot file
  --tags <TAGS>           Replace tags
  --clear-tags            Remove all tags
  --description <TEXT>    Replace description
  --clear-description     Remove description
  --exclude <PATH,...>    Exclude paths (rebuilds tree)
  --exclude-file <PATH>   File with exclude paths
```

### `mapache recall`

Recover a dropped (forgotten) snapshot.

```
mapache recall <SNAPSHOT_ID> -r <URL>
```

### `mapache rebuild-index`

Rebuild the index by scanning all packs.

```
mapache rebuild-index -r <URL>
  --dry-run               Simulate without making changes
```

### `mapache rechunk`

Rechunk all snapshots with current parameters.

```
mapache rechunk -r <URL>
```

### `mapache copy`

Transfer snapshots between repositories.

```
mapache copy --from <URL> -r <URL>
  --from <URL>                Source repository (required)
  --from-ssh-privatekey <PATH>  SSH key for source repository
  --from-ssh-known-hosts <PATH>  SSH known hosts file for source
  --snapshot <PREFIX>         Copy only snapshots with this ID prefix (repeatable)
  --host <HOST>               Copy only snapshots from this host (repeatable)
  --tags <TAGS>               Copy only snapshots with this tag (repeatable)
  --dry-run                   Show what would be copied without transferring
```

### `mapache sync`

Synchronize a repository to another backend.

```
mapache sync --target <URL> -r <URL>
  --delete                Delete files in destination not in source
  --dst-ssh-privatekey <PATH>  SSH key for destination
  --dst-ssh-known-hosts <PATH>  SSH known hosts file for destination
  --dry-run               Simulate without making changes
```

### `mapache unlock`

Remove stale locks.

```
mapache unlock -r <URL>
  -f, --force             Remove all locks (including non-expired)
```

### `mapache cache`

Manage local cache directories.

```
mapache cache
  --delete <PREFIX>...    Delete cache folders by ID prefix (repeatable)
  --clear                 Delete all cache folders
```

### `mapache completion`

Generate shell completion scripts.

```
mapache completion --shell <SHELL> --path <DIR>
  --shell <SHELL>         Shell type (bash, zsh, fish, powershell, elvish, nushell)
  --path <DIR>            Output directory
```

### `mapache config`

Manage repository configuration.

```
mapache config template [--output <PATH>]
  --output <PATH>         Write template to file instead of stdout
```

### `mapache tui`

Launch terminal user interface.

```
mapache tui -r <URL>
```

---

## 22. Troubleshooting & FAQ

### Lock Issues

**Q:** I get "repository is locked" errors. What should I do?

**A:** If a previous command crashed, run `mapache unlock -r <URL>` to remove
stale locks. For persistent issues, use `--force`. To avoid this in scripts,
use `--retry-lock` to automatically wait for locks to be released. For
read-only operations (e.g. `log`, `ls`, `find`), use `--no-lock` to skip
lock acquisition entirely.

### Performance

**Q:** Backups are slow on large file trees.

**A:** Adjust parallelism with `--readers` and `--packers`. Monitor disk I/O
and CPU usage. Consider using a faster compression level (`fastest`) for
initial backups, or `--exclude` to skip unnecessary paths.

### Repository Corruption

**Q:** I suspect my repository is corrupted.

**A:** Run `mapache verify -r <URL>` to check logical consistency or
`mapache verify --read-packs -r <URL>` for full physical verification. If
the index is corrupted but packs are intact, use `mapache rebuild-index -r <URL>`.

### Storage Space

**Q:** The repository is using more space than expected.

**A:** Run `mapache stats --full -r <URL>` to see compression ratios and
dangling blobs. Then `mapache clean -r <URL>` to run garbage collection. If
you have deleted many snapshots, consider `mapache clean --tolerance 0 -r <URL>`
for aggressive cleanup.

### Authentication

**Q:** I forgot my password. Can I recover it?

**A:** No. Mapache uses the password to derive the key that decrypts the
master key. Without the password, the data is unrecoverable. Always export
key files as a backup: `mapache key export <KEY_ID> -o ~/safe/keyfile -r <URL>`.

### Cross-Platform

**Q:** Can I use the same repository from Windows, Linux, and macOS?

**A:** Yes, mapache repositories are platform-independent. However, FUSE mount
is only available on Unix systems. Bundle files are fully portable.

### Data Safety

**Q:** Is it safe to delete old snapshots?

**A:** Yes. Deleting a snapshot only removes its metadata file. The underlying
data blobs are reference-counted by the index. Garbage collection (`clean`)
removes only blobs that are no longer referenced by any snapshot.

**Q:** Can I interrupt a snapshot in progress?

**A:** Yes. Press `Ctrl+C`. All data written so far is persisted. Unfinished
packs not yet indexed are harmless — they will be ignored and eventually
cleaned up by GC. The snapshot will not be saved.
