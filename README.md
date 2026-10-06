# mapache — Fast, Encrypted, Deduplicating Backup Tool

[![CI](https://github.com/jLantxa/mapache/actions/workflows/main.yaml/badge.svg)](https://github.com/jLantxa/mapache/actions/workflows/main.yaml)
[![crates.io](https://img.shields.io/crates/v/mapache)](https://crates.io/crates/mapache)
[![License](https://img.shields.io/crates/l/mapache)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-mdBook-blue)](https://jlantxa.github.io/mapache/)

`mapache` (Spanish for raccoon 🦝) is a high-performance, deduplicating backup
tool designed for speed, reliability, and uncompromising security. Inspired by
[`restic`](https://restic.net/) and built with Rust, it provides a modern
approach to incremental backups.

## Project Status

Mapache is a feature-complete backup solution. While the architecture is
designed for reliability and has extensive test coverage, it is a relatively
new project. As with any tool managing critical data, users should perform
their own validation before relying on it for primary backups.

> **⚠️ Repository format v1 is deprecated:** Support for the v1 repository
> format is deprecated and will be completely removed in future releases. New
> repositories use v2, and v1 repositories are still readable, so you have time
> to migrate. To upgrade a v1 repository to v2, run
> `mapache migrate -r <path/to/repo>`, adding `--dry-run` to preview the
> changes first. See the
> [manual](https://jlantxa.github.io/mapache/manual.html#migrate--migrate-repository-format)
> for details.

## Documentation

Full documentation is available at **[jlantxa.github.io/mapache](https://jlantxa.github.io/mapache/)**.

- [Manual](https://jlantxa.github.io/mapache/manual.html) — complete usage reference
- [Design](https://jlantxa.github.io/mapache/design_v2.html) — repository format and architecture
- [Design v1 (Deprecated)](https://jlantxa.github.io/mapache/design_v1.html) — legacy v1 format specification

## Key Features

- **Performance:** parallel multi-threaded backup and restore pipeline.
  Configurable concurrency for readers and packers.
- **Global deduplication:** content-defined chunking across all snapshots
  and machines. Only new data is stored.
- **Encryption:** AES-256-GCM-SIV with Argon2id key derivation. Data is
  never stored or transmitted in the clear.
- **Zero-config operation:** single self-contained release binary with
  no runtime dependencies (statically linked on Linux and Windows; macOS
  binaries link Apple's system libraries). Point it at a directory and
  run. Binaries built from source with `cargo build` link your system's
  libraries instead.
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
  individual blobs. Optional Reed-Solomon ECC sidecars detect and repair
  bit-rot.
- **Portable:** Linux, macOS, Windows and Android — single binary for each
  platform.

## Benchmarks

This is a non-exhaustive set of benchmarks run on my development hardware.
They serve as a baseline for comparing performance between versions, using
restic v0.19.1 as a base.

**Test environment:** Fedora 44, AMD Ryzen 9 3900X (24 threads), SanDisk
Extreme PRO NVMe.

Each result is the average of 3 runs following a warmup run, all on local
storage. Both tools run with default settings and 8 readers for backup.

Workloads:

- **kernel** — Linux kernel source tree (~1.6 GB, 99'131 objects)
- **enron** — Enron email corpus (~1.4 GB, 520'901 objects)

### kernel

| Tool    | Action  | Avg Time (s) | Max Time (s) | Avg PSS (MB) | Peak PSS (MB) | Avg CPU (%) | Repo (MB) |
|---------|---------|--------------|--------------|--------------|---------------|-------------|-----------|
| mapache | backup  |         1.98 |         2.02 |       358.77 |        362.04 |     1413.12 |    303.56 |
| restic  | backup  |         3.90 |         3.97 |       812.79 |        839.21 |     1284.41 |    308.86 |
| mapache | restore |        10.37 |        10.61 |       407.37 |        430.17 |      278.21 |      0.00 |
| restic  | restore |        13.96 |        14.01 |       240.26 |        250.34 |      141.76 |      0.00 |

### enron

| Tool    | Action  | Avg Time (s) | Max Time (s) | Avg PSS (MB) | Peak PSS (MB) | Avg CPU (%) | Repo (MB) |
|---------|---------|--------------|--------------|--------------|---------------|-------------|-----------|
| mapache | backup  |         4.48 |         4.51 |       385.03 |        386.79 |     1346.55 |    714.38 |
| mapache | restore |        52.53 |        53.21 |       629.37 |        645.11 |      248.09 |      0.00 |
| restic  | backup  |        10.67 |        10.77 |       858.84 |        904.03 |     1159.24 |    725.08 |
| restic  | restore |        64.71 |        64.94 |       441.92 |        451.32 |      148.16 |      0.00 |

## Getting Started

### Installation

**Option 1 — Pre-built binaries (recommended)** (Linux, macOS, Windows,
Android):

```bash
curl -fsSL https://github.com/jlantxa/mapache/raw/main/tools/install.sh | sh
```

Binary builds are also available on the
[Releases page](https://github.com/jlantxa/mapache/releases). Linux and
Windows binaries are statically linked and fully self-contained — no
runtime dependencies to install, and they run on any Linux distro or
modern Windows. macOS binaries link Apple's system libraries. Use this
when you want to be up and running in seconds, or need a binary to
deploy on a machine that has no build toolchain.

**Option 2 — Install from crates.io**:

```bash
cargo install mapache
```

Builds the latest published version from source and links against your
system's libraries (glibc on Linux, system frameworks on macOS). Requires
the [Rust toolchain] and a C toolchain, but no other build dependencies.
Prefer this when you want a specific version (`--version 0.7.1`), rebuild
regularly, or prefer to audit and control exactly what the binary links
against.

> **macOS:** the default `mount` feature needs FUSE on your machine, which
> is up to you to install — `brew install --cask macfuse`. This is the same
> deal as Linux, where mounting needs the system's `fuse3` package. macFUSE
> is required both to build and to run; without it, build without mount:
> `cargo install mapache --no-default-features`. Pre-built binaries (Option 1)
> include `mount` and only need macFUSE at runtime.

**Option 3 — Build from source**:

[Rust toolchain]: https://rustup.rs/

```bash
# Development build
cargo build --release

# Install a local build into your cargo bin path
cargo install --path crates/mapache

# Fully static, self-contained build (Linux/Windows)
make release-static
```

`cargo build` compiles a binary linked against your system's libraries.
This is fine for testing and development on the same hardware; for a
portable static binary, run `make release-static` or use Option 1.

> **Feature flags:** `mount` is enabled by default (Unix). The FUSE
> support on your system is a user-provided prerequisite — Linux needs the
> `fuse3` package (uses the `fusermount` helper at runtime); macOS needs
> macFUSE, both to build and to run. To build without it, use
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
