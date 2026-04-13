# mapache

![Badge](https://github.com/jlantxa/mapache/workflows/main/badge.svg)

Mapache is a **fast, secure, deduplicating, incremental backup tool**
written in Rust.

You can find more [in-depth documentation](doc/mapache.md).

---

## Table of Contents

- [About](#about)
- [Key Features](#key-features)
- [Roadmap](#roadmap)
- [Getting Started](#getting-started)

---

## About

`mapache` (Spanish for raccoon 🦝) is a high-performance, deduplicating backup
tool designed for speed, reliability, and uncompromising security. Inspired by
[`restic`](https://restic.net/) and built with Rust, it provides a modern
approach to incremental backups.

At its core, `mapache` operates on a content-addressable repository model. Every
file, directory, and piece of metadata is decomposed into binary objects
identified by their BLAKE3 cryptographic hash. This architecture naturally
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

## Key Features

- **Deduplication:** FastCDC (Content-Defined Chunking) identifies shifted
  data to minimize storage footprint.
- **Security:** Mandatory AES-GCM-SIV encryption and Argon2 KDF — your data is
  never stored or transmitted in the clear.
- **Compression:** Zstd compression with adjustable levels to balance backup
  speed and storage usage.
- **Backends:** Native support for Local FS, SFTP, and S3 (experimental).
- **Portable:** A single, statically linked binary with zero external
  dependencies.

## Roadmap

### v0.1.0

mapache 0.1.0 was the first public stable release. It was meant to be a first
stable prototype with all core features after 8 months of work.

The v0.1.x series brought bug fixes, optimizations and minor new features.
The main goal was to optimize the Archiver performance.

### v0.2.0

The v0.2.0 marks the finalization of the Archiver. The complete engine was
refactored to make the backend code async. An initial S3 backend implementation
was added and the SFTP backend was reimplemented with a rust-native async crate.
The async refactor had the additional challenge of tuning parallelism to trim
down memory usage while maintaining performance.

### v0.3.0 (_we are here_)

Redesigned the restorer into a high-performance, pack-centric engine with
background prefetching and concurrent restoration to significantly improve
I/O efficiency.

- [x] `restore` redesign

### Future

All other planned features:

- [ ] configuration files,
- [ ] master key rotation,
- [ ] return codes for commands,

and more.

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
