# mapache — Fast, Encrypted, Deduplicating Backup Tool

![Badge](https://github.com/jlantxa/mapache/workflows/main/badge.svg)

`mapache` (Spanish for raccoon 🦝) is a high-performance, deduplicating backup
tool designed for speed, reliability, and uncompromising security. Inspired by
[`restic`](https://restic.net/) and built with Rust, it provides a modern
approach to incremental backups.

## Project Status

Mapache is a feature-complete backup solution. While the architecture is
designed for reliability and has extensive test coverage, it is a relatively
new project. As with any tool managing critical data, users should perform
their own validation before relying on it for primary backups.

## Documentation

Full documentation is available at **[mapache.jlantxa.dev](https://jlantxa.github.io/mapache/)**.

- [Manual](https://jlantxa.github.io/mapache/manual.html) — complete usage reference
- [Design](https://jlantxa.github.io/mapache/design.html) — repository format and architecture

## Key Features

- **Performance:** parallel multi-threaded backup and restore pipeline.
  Configurable concurrency for readers and packers.
- **Global deduplication:** content-defined chunking across all snapshots
  and machines. Only new data is stored.
- **Encryption:** AES-256-GCM-SIV with Argon2id key derivation. Data is
  never stored or transmitted in the clear.
- **Zero-config operation:** single statically-linked binary, no
  dependencies. Point it at a directory and run.
- **Backends:** local filesystem, SFTP, and S3-compatible object storage.
- **Terminal UI:** interactive TUI with dashboard, snapshot and restore
  wizards, file explorer, diff viewer, and search across snapshots.
- **Restore with control:** filter by path or glob pattern, sync mode
  preserves metadata and permissions. Coalesced reads for efficient
  transfer.
- **FUSE mount:** browse snapshot contents and bundle files as a mounted
  filesystem (Unix).
- **Bundles:** self-contained encrypted `.mapache` files with
  deduplication. Usable for transfer, shipping, or cold storage without
  access to the repository. Export snapshots to bundles and import them
  into other repositories for cross-system deduplication.
- **Multi-client:** non-exclusive locking allows multiple hosts to back up
  to the same repository simultaneously.
- **Retention policies:** keep what matters with hourly, daily, weekly,
  monthly, and yearly rules. Filter by host and tag.
- **Integrity verification:** end-to-end check of snapshots, packs, and
  individual blobs.
- **Portable:** Linux, macOS, Windows and Android — single binary for each
  platform.

## Benchmarks

This is a non-exhaustive set of benchmarks run on my development hardware.
They serve as a baseline for comparing performance between versions, using
restic v0.19.1 as a base.

**Test environment:** Fedora 44, AMD Ryzen 9 3900X (24 threads), SanDisk
Extreme PRO NVMe.

Each result is the average of 5 runs following a warmup run, all on local
storage. Both tools run with default settings and 8 readers for backup.

Workloads:

- **kernel** — Linux kernel source tree (~1.6 GB, 99'131 objects)
- **enron** — Enron email corpus (~1.4 GB, 520'901 objects)

### kernel

| Tool    | Action  | Avg Time (s) | Max Time (s) | Avg PSS (MB) | Peak PSS (MB) | Avg CPU (%) | Repo (MB) |
|---------|---------|--------------|--------------|--------------|---------------|-------------|-----------|
| mapache | backup  |         2.06 |         2.13 |       307.48 |        315.11 |     1358.59 |    304.14 |
| restic  | backup  |         3.98 |         4.19 |       834.90 |        868.00 |     1236.80 |    308.91 |
| mapache | restore |         9.03 |         9.36 |       410.87 |        426.94 |      387.96 |     --    |
| restic  | restore |        16.21 |        16.28 |       233.82 |        253.54 |      149.75 |     --    |

### enron

| Tool    | Action  | Avg Time (s) | Max Time (s) | Avg PSS (MB) | Peak PSS (MB) | Avg CPU (%) | Repo (MB) |
|---------|---------|--------------|--------------|--------------|---------------|-------------|-----------|
| mapache | backup  |         4.20 |         4.24 |       428.77 |        448.36 |     1390.57 |    717.27 |
| restic  | backup  |        10.60 |        10.75 |       845.51 |        881.58 |     1164.16 |    725.20 |
| mapache | restore |        40.41 |        41.90 |       496.44 |        517.71 |      395.50 |     --    |
| restic  | restore |        73.35 |        73.66 |       451.99 |        459.99 |      156.32 |     --    |

## Getting Started

### Installation

**Quick install** (Linux, macOS, Windows):

```bash
curl -fsSL https://github.com/jLantxa/mapache/raw/main/tools/install.sh | sh
```

Or compile from source with the [Rust toolchain]:

[Rust toolchain]: https://rustup.rs/

```bash
cargo build --release
cargo install --path crates/mapache
```

`cargo build` compiles binaries with some dynamically linked dependencies.
While this is fine for testing and development on the same hardware, if
you need a statically linked binary (which I strongly recommend for
portability), run `make release-static` or use the binaries provided in
the `Releases` page for a specific released version.

> **Note for Linux users:** The `mount` command requires FUSE development
> headers (e.g., `libfuse-dev`). To build without FUSE support, use
> `--no-default-features`.

### Quick Start

#### **Initialize a repository** (local, S3, or SFTP)

  ```bash
  # Local directory
  mapache init -r /path/to/repo

  # SFTP server
  mapache init -r sftp://user@host/backup-folder

  # S3 Bucket
  mapache init -r s3://my-bucket/backup-folder
  ```

#### **Create your first snapshot**

  ```bash
  mapache snapshot ~/Documents -r /path/to/repo
  ```

#### **Launch the TUI**

  ```bash
  mapache tui -r /path/to/repo
  ```

#### **List snapshots**

  ```bash
  mapache log -c -r /path/to/repo
  ```

#### **Restore data**

  ```bash
  mapache restore --target /tmp/restore-folder -r /path/to/repo
  ```
