# mapache

Fast, encrypted, deduplicating backup tool. Written in Rust.

## Install

```bash
curl -fsSL https://github.com/jlantxa/mapache/raw/main/tools/install.sh | sh
```

Or grab a binary from the [releases page](https://github.com/jlantxa/mapache/releases).

## Usage

```bash
# Initialize a repository
mapache init -r /path/to/repo

# Create a snapshot
mapache snapshot ~/Documents -r /path/to/repo

# Launch the TUI
mapache tui -r /path/to/repo

# List snapshots
mapache log -c -r /path/to/repo

# Restore
mapache restore --target /tmp/restore -r /path/to/repo
```

## Features

- **Parallel backup and restore** — Multi-threaded pipeline with configurable concurrency for readers and packers.
- **Encryption** — AES-256-GCM-SIV with Argon2id key derivation. Data is never stored or transmitted in the clear.
- **Deduplication** — Content-defined chunking (FastCDC) across all snapshots and machines. Only new data is stored.
- **Backends** — Local filesystem, SFTP, and S3-compatible object storage.
- **Terminal UI** — Interactive TUI with dashboard, snapshot and restore wizards, file explorer, and diff viewer.
- **Bundles** — Self-contained encrypted `.mapache` files for transfer or cold storage.
- **FUSE mount** — Browse snapshot contents as a mounted filesystem (Unix).
- **Retention policies** — Hourly, daily, weekly, monthly and yearly rules. Filter by host and tag.
- **Multi-client** — Non-exclusive locking allows multiple hosts to back up to the same repository.
- **Portable** — Linux, macOS, Windows and Android. Single statically-linked binary for each platform.
