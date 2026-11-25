# Changelog

## Unreleased

### Changed

- `ChunkStream` now cuts the data already buffered before asking the source for
  more input, instead of refilling to `max_size` and compacting on every chunk.
  The rolling hash depends only on the bytes up to the cut, so boundaries are
  unchanged.
- The lookup-table generator is now an example instead of a binary, so `blake3`
  and `rand` are dev-only dependencies and are not built by crate consumers.

### Fixed

- The streaming read buffer is now always initialized before being handed to a
  `Read` implementation. Passing uninitialized spare capacity as `&mut [u8]`
  was undefined behavior because readers may inspect the whole slice.
- `ChunkStream` now stops after reporting an I/O error instead of resuming and
  yielding chunks from data read afterwards.

## v0.3.1 (2026-09-19)

### Changed

- `ChunkStream` now grows its read buffer geometrically (doubling, capped at the
  maximum chunk size) instead of allocating the full `max_size` buffer upfront.
  Small and medium sources no longer pay for a needlessly large allocation.

### Fixed

- `Chunker::new` now rounds `normal_size` to the nearest log2 bucket instead of
  flooring it, matching fastcdc's behaviour. Non-power-of-two `normal_size`
  values previously selected a mask bucket one step too small.

## v0.3.0 (2026-08-26)

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
