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
- [Roadmap](#roadmap)

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
v0.18.1 as a base.

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
| mapache | backup  |         2.06 |         2.16 |       303.23 |        311.23 |     1344.65 |    304.15 |
| mapache | restore |         8.51 |         8.61 |       415.20 |        429.18 |      412.23 |     --    |
| restic  | backup  |         4.13 |         4.65 |       825.88 |        849.96 |     1197.60 |    308.84 |
| restic  | restore |        15.77 |        15.81 |       256.42 |        272.48 |      149.65 |     --    |

### enron

| Tool    | Action  | Avg Time (s) | Max Time (s) | Avg PSS (MB) | Peak PSS (MB) | Avg CPU (%) | Repo (MB) |
|---------|---------|--------------|--------------|--------------|---------------|-------------|-----------|
| mapache | backup  |         4.47 |         4.51 |       425.24 |        442.99 |     1322.17 |    717.26 |
| mapache | restore |        40.10 |        41.14 |       566.07 |        572.32 |      380.97 |     --    |
| restic  | backup  |        10.86 |        11.26 |       859.05 |        863.23 |     1124.77 |    725.07 |
| restic  | restore |        71.04 |        71.36 |       449.26 |        465.44 |      156.66 |     --    |

## Getting Started

### Installation

To compile `mapache` from source, ensure you have the [Rust toolchain] installed,
then build and install the binary:

[Rust toolchain]: https://rustup.rs/

```bash
# Build the optimized release binary
cargo build --release

# Install it to your cargo bin path
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

## Roadmap

### v0.1.0

mapache 0.1.0 was the first public stable release. It was meant to be a first
stable prototype with all core features. This version validated the
architecture.

### v0.2.0

v0.2.0 marks the finalization of the Archiver. The complete engine was
refactored to make the backend code async. An initial S3 backend implementation
was added and the SFTP backend was reimplemented with a rust-native async crate.
The async refactor had the additional challenge of tuning parallelism to trim
down memory usage while maintaining performance.

### v0.3.0

Redesigned the restorer into a high-performance, pack-centric engine with
background prefetching and concurrent restoration to significantly improve
I/O efficiency.

- [x] `restore` redesign
- [x] Multi-platform static builds (Linux x64/ARM, Windows, macOS)
- [x] Return codes for commands
- [x] Security hardening (secure join, zeroize, FUSE permissions)

### v0.4.0

Focused on new tooling, configuration, and performance optimizations.

- [x] **Bundle command** — Self-contained `.mapache` bundle files with
  deduplication, encryption, and FUSE mount capability.
- [x] **TOML config file** — Centralized repository settings with `--config` flag.
- [x] **Enhanced retention** — `--host`, `--keep-hourly`, `--keep-min` for the
  `forget` command.
- [x] **S3 multipart uploads** — For files >= 128 MiB.
- [x] **Access time preservation** — `--with-atime` flag for snapshot command.
- [x] **JSON output** for `clean`, `stats`, and other commands.
- [x] **Experimental TUI** — Interactive terminal interface for snapshot
  creation, restore, and retention management (may be removed or replaced).

### Future

In the future, I want to polish all rough edges, like adding `json` output and
error codes to all commands. The current TUI is experimental and may be
replaced by a separate frontend that consumes the JSON output.

Other planned features (non-exhaustive):

- [ ] incremental restore
- [ ] master key rotation
