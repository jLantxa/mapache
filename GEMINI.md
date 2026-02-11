# Mapache Context for Gemini

## Project Overview

**Mapache** is a fast, secure, de-duplicating, incremental backup tool written in Rust. It draws inspiration from `git` and `restic`, utilizing content-addressable storage and content-defined chunking (FastCDC) to efficiently manage backups as snapshots.

### Key Features
*   **Deduplication:** Uses FastCDC to chunk files and minimize storage usage.
*   **Encryption:** Mandatory encryption using AES-GCM-SIV with Argon2 for key derivation.
*   **Snapshots:** Independent, immutable representations of the filesystem at a point in time.
*   **Self-contained:** Aims to be a static binary with no runtime dependencies (except optional FUSE).

## Architecture

The project is a Rust workspace with the following members:

*   **`core` (package `mapache`):** The main application logic.
    *   `src/commands/`: CLI command implementations (e.g., `backup`, `restore`, `mount`).
    *   `src/backend/`: Storage backend implementations (Local FS, SFTP).
    *   `src/repository/`: Core repository logic (indexing, packing, garbage collection).
    *   `src/fuse/`: FUSE filesystem implementation for mounting snapshots.
*   **`chunker`:** A library crate implementing the chunking logic.
    *   Contains the FastCDC algorithm implementation.

## Building and Running

### Prerequisites
*   **Rust:** Latest stable version.
*   **Perl:** Required for building `openssl`.
*   **FUSE:** Required for the `mount` command (on Linux). Can be disabled via features.

### Common Commands (via Makefile)
*   **Build (Debug):** `make debug` (runs `cargo build`)
*   **Build (Release):** `make release` (runs `cargo build --release`)
*   **Test:** `make test` (runs `cargo test`)
*   **Lint:** `make clippy` (runs `cargo clippy`)
*   **Format:** `make fmt` (runs `cargo fmt`)
*   **Documentation:** `make doc`

### Manual Commands
*   **Run:** `cargo run --bin mapache -- <args>`
*   **Build without FUSE:** `cargo build --release --no-default-features`

## Development Conventions

*   **Style:** Follows standard Rust formatting (`rustfmt`).
*   **Linting:** `clippy` should be used to ensure code quality.
*   **Testing:**
    *   Unit tests are co-located with code in `src/`.
    *   Integration tests are located in `core/tests/integration_tests/`.
    *   Use `make test` to run all tests.
*   **Dependencies:** Managed via `Cargo.toml`. `openssl` is vendored in `ssh2` or built from source.

## Directory Structure Highlights
*   `core/`: Main CLI and logic.
*   `chunker/`: Chunking algorithms.
*   `doc/`: Design documents and user manual.
*   `tools/`: Helper scripts (e.g., `cat.py`).
