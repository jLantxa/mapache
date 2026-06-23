# mapache — Fast, Encrypted, Deduplicating Backup Tool

![Badge](https://github.com/jlantxa/mapache/workflows/main/badge.svg)


Mapache is a **fast, secure, deduplicating, incremental backup tool**
written in Rust.

You can find more [in-depth documentation](doc/mapache.md).

---

## Table of Contents

- [About](#about)
- [Key Features](#key-features)
- [Benchmarks](#benchmarks)
- [Getting Started](#getting-started)

---

## About

`mapache` (Spanish for raccoon 🦝) is a high-performance, deduplicating backup
tool designed for speed, reliability, and uncompromising security. Inspired by
[`restic`](https://restic.net/) and built with Rust, it provides a modern
approach to incremental backups.

At its core, `mapache` operates on a content-addressable repository model. Every
file, directory, and piece of metadata is decomposed into binary objects
identified by their cryptographic hash. This architecture naturally
enables global deduplication: if multiple files share the same content—even
across different snapshots or machines — `mapache` stores that data only once.
To ensure storage efficiency and high I/O throughput, these objects are bundled
into "pack files" and tracked via a central index that allows for near-instant
lookups and atomic repository updates.

Each backup is captured as a "Snapshot" representing a complete, point-in-time
state of your file system. Unlike traditional backup tools that rely on complex
"full vs. incremental" chains, every `mapache` snapshot is technically
independent but shares underlying data blobs with others. This means you can
delete any old snapshot at any time without risking the integrity of newer
ones. All data, from file contents to directory structures, is compressed with
`zstd` and protected by AES-GCM-SIV authenticated encryption, ensuring your
repository remains a "black box" to anyone without the master key.

## **Project Status**

Mapache is a feature-complete backup solution. While the architecture is
designed for reliability and has extensive test coverage, it is a relatively new
project. As with any tool managing critical data, users should perform their own
validation before relying on it for primary backups.

---

## Key Features

- **Deduplication:** FastCDC (Content-Defined Chunking) identifies shifted
  data to minimize storage footprint.
- **Security:** Mandatory AES-GCM-SIV encryption and Argon2 KDF — your data is
  never stored or transmitted in the clear.
- **Compression:** Zstd compression with adjustable levels to balance backup
  speed and storage usage.
- **Terminal UI:** Rich interactive TUI with dashboard, snapshot/restore
  screens, file explorer, diff viewer, and live search across snapshots.
- **Backends:** Native support for Local FS, SFTP, and S3.
- **Portable:** A single, statically linked binary with zero external
  dependencies.
- **Verifiable**: Verify all snapshots, packs, and blobs to make sure your data
  can be restored at any time.
- **TOML Config:** Centralized repository settings via a `.toml` configuration
  file, overridable with CLI flags.
- **Bundle Files:** Self-contained `.mapache` bundle format with deduplication,
  encryption, and FUSE mount support for secure data transfer.
- **Flexible Retention:** Policy-based snapshot retention with hourly, daily,
  weekly, monthly, yearly rules, plus host and tag filtering.

## Benchmarks

This is a non-exhaustive set of benchmarks run on my development hardware. They
serve as a baseline for comparing performance between versions, using restic
v0.19.0 as a base.

**Test environment:** Fedora 44, AMD Ryzen 9 3900X (24 threads), SanDisk Extreme
PRO NVMe.

Each result is the average of 3 runs following a warmup run, all on local
storage. Both tools are run with default settings and 8 readers
(read-concurrency) for backup.

Mapache has traditionally been slower with datasets made of many small files, so
this benchmark test addresses that area specifically.

Workloads:

- **kernel** — Linux kernel source tree (~1.6 GB, 99'131 objects)
- **enron** — Enron email corpus (~1.4 GB, 520'901 objects)


### kernel

| Tool    | Action  | Avg Time (s) | Max Time (s) | Avg PSS (MB) | Peak PSS (MB) | Avg CPU (%) | Repo (MB) |
|---------|---------|--------------|-------------|---------------|---------------|-------------|-----------|
| mapache | backup  |         2.06 |         2.13 |       307.48 |        315.11 |     1358.59 |    304.14 |
| mapache | restore |         9.03 |         9.36 |       410.87 |        426.94 |      387.96 |     --    |
| restic  | backup  |         3.98 |         4.19 |       834.90 |        868.00 |     1236.80 |    308.91 |
| restic  | restore |        16.21 |        16.28 |       233.82 |        253.54 |      149.75 |     --    |

### enron

| Tool    | Action  | Avg Time (s) | Max Time (s) | Avg PSS (MB) | Peak PSS (MB) | Avg CPU (%) | Repo (MB) |
|---------|---------|--------------|--------------|--------------|---------------|-------------|-----------|
| mapache | backup  |         4.20 |         4.24 |       428.77 |        448.36 |     1390.57 |    717.27 |
| mapache | restore |        40.41 |        41.90 |       496.44 |        517.71 |      395.50 |     --    |
| restic  | backup  |        10.60 |        10.75 |       845.51 |        881.58 |     1164.16 |    725.20 |
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
cargo install --path core
```

`cargo build` compiles binaries with some dynamically linked dependencies. While
this is fine for testing and development on the same hardware, if you need a
statically linked binary (which I strongly recommend for portability), run
`make release-static` or use the binaries provided in the `Releases` page for a
specific released version.

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

#### **List snapshots**

  ```bash
  mapache log -c -r /path/to/repo
  ```

#### **Restore data**

  ```bash
  mapache restore --target /tmp/restore-folder -r /path/to/repo
  ```
