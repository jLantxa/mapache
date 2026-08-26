# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- `cut()` now returns `(u64, usize)` instead of `usize`. The second element is the
  cut point offset (same as before); the first is the Gear fingerprint hash at that
  position. This avoids recomputing the hash when the caller needs both values.

- Size assertions (`min_size`, `normal_size`, `max_size` must be even) now use
  `assert!` instead of `debug_assert!`. Odd values panic in both debug and release
  builds, and produce a compile error when used in const contexts.

### Removed

- Dead odd-alignment branch in `cut()` — removed since all sizes are now enforced
  even at construction time.

- `size_hint()` on `ChunkStream` — the implementation returned `(0, None)` which
  provided no useful information for a `Read`-based streaming iterator.

### Added

- Criterion benchmark suite (`crates/chunker/benches/chunking.rs`) covering
  multiple content types, chunk sizes, normalization levels, and isolated
  scan-loop measurements.

## [0.2.4] - 2025-08-22

### Changed

- Initial crates.io publication.
