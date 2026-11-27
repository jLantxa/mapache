# mapache — Secure, Reliable, Efficient Backup Tool

[![CI](https://github.com/jLantxa/mapache/actions/workflows/main.yaml/badge.svg)](https://github.com/jLantxa/mapache/actions/workflows/main.yaml)
[![crates.io](https://img.shields.io/crates/v/mapache)](https://crates.io/crates/mapache)
[![License](https://img.shields.io/crates/l/mapache)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-mdBook-blue)](https://jlantxa.github.io/mapache/)

`mapache` (Spanish for raccoon 🦝) is a backup and storage tool designed to be
secure, reliable and efficient.

It keeps data private, verifies what it stores, and gives it back intact.

## Project Status

Mapache is a relatively new tool. Its storage system is complete and tested;
as with any backup tool, the sensible approach is to run it for a while in your
own environment and verify restores with your own data before relying on it for
primary backups.

## Documentation

Full documentation is available at **[jlantxa.github.io/mapache](https://jlantxa.github.io/mapache/)**.

- [Manual](https://jlantxa.github.io/mapache/manual.html) — complete usage reference

## Compatibility

Mapache uses repository format v2. It is treated as a stable contract: future
changes must be additive or arrive as a new format with an explicit migration.
Readers reject unknown values instead of guessing. See the
[format design](https://jlantxa.github.io/mapache/design_v2.html).

## Key Features

- **Encryption:** AES-256-GCM-SIV with Argon2id key derivation. Data is
  never stored or transmitted in the clear.
- **Integrity verification:** end-to-end check of snapshots, packs, and
  individual blobs. Optional Reed-Solomon ECC sidecars detect and repair
  bit-rot.
- **Global deduplication:** content-defined chunking across all snapshots
  and machines. Only new data is stored.
- **Compression policy:** zstd with adjustable levels, automatic skipping of
  incompressible data, and per-glob overrides. `repack` and `rechunk` apply
  new settings to existing data.
- **Adaptive parallelism:** backup, restore and maintenance derive their
  concurrency from detected CPUs and available memory (cgroup-aware on Linux),
  sharing one resource budget across the pipeline. A big machine gets more
  work in flight; a small one stays inside its limits. The automatic budget is
  70% of available memory, and explicit overrides are available when you know
  better.
- **Zero-config operation:** single self-contained release binary with
  no runtime dependencies (statically linked on Linux and Windows; macOS
  binaries link Apple's system libraries). Point it at a directory and
  run. Binaries built from source with `cargo build` link your system's
  libraries instead.
- **Backends:** local filesystem, SFTP, and S3-compatible object storage.
- **Restore with control:** filter by path or glob pattern, sync mode
  preserves metadata and permissions. Coalesced reads for efficient
  transfer.
- **Windows volume snapshots:** `--use-fs-snapshot` reads sources through a
  VSS shadow copy, capturing files held open by running programs (best effort).
- **Retention policies:** keep what matters with hourly, daily, weekly,
  monthly, and yearly rules. Filter by host and tag, or apply rules
  independently to groups of snapshots with `--group-by`.
- **Multi-client:** non-exclusive locking allows multiple hosts to back up
  to the same repository simultaneously.
- **Bundles:** self-contained encrypted `.mapache` files with
  deduplication. Usable for transfer, shipping, or cold storage without
  access to the repository. Export snapshots to bundles and import them
  into other repositories for cross-system deduplication.
- **Terminal UI:** interactive TUI with dashboard, snapshot and restore
  wizards, file explorer, diff viewer, command palette, and search across
  snapshots.
- **FUSE mount:** browse snapshot contents and bundle files as a mounted
  filesystem (Unix).
- **Portable:** Linux, macOS, Windows and Android — single binary for each
  platform.

## Getting Started

### Installation

**Option 1 — Pre-built binaries (recommended)** (Linux, macOS, Windows,
Android):

```bash
curl -fsSL https://github.com/jlantxa/mapache/raw/main/tools/install.sh | sh
```

Binary builds are also available on the
[Releases page](https://github.com/jlantxa/mapache/releases). Use this when you
want to be up and running in seconds, or need a binary to deploy on a machine
that has no build toolchain. Linking details are in
[Key Features](#key-features).

**Option 2 — Install from crates.io**:

```bash
cargo install mapache
```

Builds the latest published version from source and links against your
system's libraries (glibc on Linux, system frameworks on macOS). Requires
the [Rust toolchain] and a C toolchain, but no other build dependencies.
Prefer this when you want a specific version (`--version <VERSION>`), rebuild
regularly, or prefer to audit and control exactly what the binary links
against.

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
Official release binaries are built with the pinned Docker image using
`python3 tools/docker/build.py --ref <version>`.

> **FUSE / mount:** `mount` is enabled by default on Unix. The FUSE support is
> a system prerequisite: Linux needs the `fuse3` package (uses `fusermount` at
> runtime); macOS needs macFUSE to build and run
> (`brew install --cask macfuse`). Pre-built binaries (Option 1) include
> `mount` and only need FUSE at runtime. To build without it, use
> `--no-default-features`.

### Quick Start

#### **Initialize a repository** (local, S3, or SFTP)

```bash
# Local directory
mapache init -r /path/to/repo

# SFTP server
mapache init -r sftp://user@host/backup-folder

# S3 bucket
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

#### **Verify the backup**

```bash
mapache verify --read-packs -r /path/to/repo
```

For a restore drill, use a throwaway target such as `/tmp/mapache-restore-test`
and compare one restored file with the original. `verify --repair` uses ECC
sidecars when present.
