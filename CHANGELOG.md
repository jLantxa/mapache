# Changelog

## Unreleased

### Added

- **Repository format v2**: New repository format with compact binary serialization
  for index files, replacing JSON from v1. Tree blobs use JSON (same as v1) for
  forward-compatible extensibility. v1 is deprecated and will be removed in a
  future release.
- **`mapache migrate`**: New command to convert v1 repositories to v2 format. Handles
  re-encryption of packs, file re-indexing, and tree re-serialization. Supports
  `--dry-run` for preview.
- **`--format`**: New flag in `init` to select repository format version.
- **`--compression none`**: Per-blob compression marker in v2 enables storing
  already-compressed content (video, photos, archives) without zstd overhead.
- **Zero blob deduplication**: New `BlobType::Zero` deduplicates zero-filled regions
  across snapshots. Zero blobs are stored in pack footers with length=0, consuming
  no pack space.
- **ECC (Reed-Solomon error correction)**: Optional parity-only sidecars (`.ecc` files)
  for pack files, configured via `--ecc <PERCENT>` during `init`. Striped encoding
  handles multi-gigabyte packs without loading the entire file into memory.
- **ECC in bundle format v2**: Bundle files now support inline Reed-Solomon ECC
  protecting the blob data section. Enabled with `--ecc <PERCENT>` during bundle
  creation. Corrupted blobs are automatically repaired during extraction.
- **`mapache ecc`**: New command to manage ECC sidecars after repository creation.
  Subcommands: `enable`, `disable`, `set-percent`, and `regenerate`. Allows
  enabling ECC on existing repositories, changing the overhead percentage, or
  regenerating all sidecars.
- *Verify metadata*: `mapache verify` can now check metadata files (index,
  snapshot, etc.) and repair them if ECC is enabled.
- **Lazy index loading**: Index files are now loaded on demand, reducing RAM usage
  for commands that don't need the full index.
- *KDF calibration*: Added `--calibrate-kdf` to allow calibrating the Argon2id
  parameters for a target run time in the running hardware. Memory is
  auto-detected (10% of RAM, clamped to 32–64 MiB).

### Changed

- **Restorer memory**: Reduced restore memory usage by compacting the per-blob
  planning data and eliminating redundant allocations in the pack download pipeline.
- **LRU cache evicts by blob count**: In lazy mode, the cold index LRU cache now
  evicts entries based on total blob count instead of index count. This provides
  more granular memory control — a single index with 65k blobs costs proportionally
  more than one with 1k blobs. Configurable via `[runtime] lru-max-blobs` in the
  TOML config (default: 1,000,000).
- **AES-GCM-SIV nonce position**: In v2, encrypted blobs place the nonce at the end
  (`[ct | tag | nonce]`) instead of the start. Eliminates an extra allocation and
  memory copy during encryption.
- **v1 deprecation warning**: `snapshot`, `restore`, and other commands now warn when
  operating on v1 repositories. Consider migrating with `mapache migrate`.
- **Bundle performance**: Speed up bundle writer and refactor archiver pipeline.
- **Keyfile format**: Key files now use a nested `kdf` object with an
  `algorithm` discriminator (e.g. `{"algorithm": "argon2id", "m": ..., "t": ..., "p": ...}`)
  instead of flat top-level `m`, `t`, `p` fields. Old v1 keyfiles are read
  transparently by constructing the `kdf` object from the flat fields.

### Fixed

- **Config precedence**: CLI flags now correctly take precedence over config
  file values. Previously any value present in the config silently overrode the
  explicit command-line flag (e.g. config `keep_last=1` won over
  `--keep-last 30`).
- **Restore metadata safety**: With `--strategy skip` or `--strategy newer`,
  files kept locally no longer get their metadata (mtime, permissions, xattrs)
  silently overwritten by the snapshot's metadata. Newly restored files and
  directories still receive the snapshot's metadata.
- **`migrate` lock**: `mapache migrate` now holds an exclusive lock while
  rewriting the repository, preventing races with concurrent snapshots.
- **Copy integrity**: `mapache copy` now aborts (non-zero exit) when a source
  tree cannot be read, instead of writing a destination snapshot that silently
  references data which was never copied.
- **`rebuild-index` safety**: When any pack fails to parse, `rebuild-index`
  aborts and leaves the existing index untouched instead of persisting a
  partial index and deleting the original (which would orphan blobs). It also
  now holds an exclusive lock.
- **Blob verification**: Every blob read path (restore, cat/mount, diff, copy,
  GC, bundle extraction) now verifies that the decoded content matches its
  content ID, catching same-length blob swaps that pass AEAD decryption.
- **Keyfile hardening**: Argon2id parameters read from keyfiles are validated
  against sane bounds before any derivation, and keyfile decompression is
  capped at 64 MiB, preventing memory/CPU DoS via a planted malicious
  keyfile. Bundle headers get the same Argon2 parameter bounds check.
- **Data safety**: Skip files with missing blobs instead of silently corrupting
  them via offset drift. Validate decoded blob lengths against the index.
- **Incremental metadata**: Record `chmod`/`chown`/`xattr`/flag changes that
  were silently lost when the `Unchanged` path overwrote fresh metadata.
- **Crash-safe renames**: Fsync the parent directory after every rename so
  directory entries survive power loss. Temp files now append `.tmp` instead
  of replacing the extension, preventing sibling collisions.
- **Pre-1970 timestamps**: Preserve negative timestamps instead of clamping
  to epoch, fixing silent metadata loss on old files.
- **Bundle validation**: Reject blob sizes and trailer fields that overflow
  `u32` instead of silently truncating.
- **Bundle cleanup on interrupt/error**: Delete the output file when a bundle
  creation is interrupted by Ctrl+C or fails after the file has been created.
  Also delay opening the bundle writer until after all upfront checks pass.
- **Verify exit codes**: Detect corrupt metadata files (exit code `23`) and
  preserve per-error exit codes in `clean`.
- **Symlink restore safety**: Symlink subtrees no longer emit during tree
  walk, preventing writes through a symlink from escaping the target root.
- **Cache race**: Fix missed wakeups in download coalescing; failed downloads
  now fail fast for all waiters.
- **Lazy index**: Resolve cold zero blobs without a disk load and sort
  zero-blob metadata for binary search.
- **SFTP/S3**: Non-blocking connection acquisition; stop retrying permanent
  HTTP 4xx errors. Rate limiter now throttles reads in chunks.
- Reduced lock hold time in GC referenced blob scanning.
- Windows: expand `~` when `HOME` is not set.

## v0.6.0 (2026-07-31)

### Fixed

- On Windows, files modified within the same second as the previous snapshot
  were misclassified as unchanged, causing stale data on restore with `--verify`.
  Fixed by using exact timestamp comparison in the archiver.
- Using `--keep-yearly all` (and `--keep-monthly`, `--keep-weekly`,
  `--keep-daily`, `--keep-hourly`) deleted all snapshots instead of keeping one
  per period. Fixed by short-circuiting the arithmetic overflow caused by the
  internal `usize::MAX` value.
- `--keep-last 0` was accepted and kept zero snapshots, causing all snapshots to
  be removed. Now rejected with a clear error. `--keep-last all` is also now
  accepted.
- The `restore_complete` JSON event always reported zero errors, warnings,
  items, and bytes even when the restore had non-zero counts. The summary
  counters now reflect the actual restore progress.
- Passing `0` to `--parallel` (verify), `--readers` (snapshot, bundle), or
  `--packers` (snapshot) caused hangs or panics. Now rejected at parse time
  with a clear error.
- Setting `gc-repack-concurrency`, `restore-blob-concurrency`,
  `restore-pack-prefetch`, `s3-multipart-part-size`, `snapshot.num-packers`,
  or `snapshot.num-readers` to `0` in the config file caused hangs or infinite
  loops. Now rejected at config load with a clear error.
- Setting `pack-size-mib` to `0` or values outside the supported range was
  silently accepted and could cause panics or misbehavior. Now rejected at
  config load with a clear error.
- A `MutexGuard` held across an `.await` point in the lock refresh handler
  could cause deadlocks. Also fixed: spawned tasks panicking silently in
  `Drop`, and the archiver `join!` discarding task errors.
- `--sample` in `verify` now requires `--read-packs` to take effect.
- Error messages in `verify`, `snapshot`, `completion`, and `cache` used Debug
  format (`{:?}`) for paths and error types, producing ugly output for users.
  Now uses Display format throughout.
- The `--keep-yearly` / `--keep-monthly` / `--keep-weekly` / `--keep-daily` /
  `--keep-hourly` help text incorrectly stated N must be "greater than 1".
  Corrected to "greater than 0", matching actual validation.
- `mount` interrupt message was inconsistent with other commands (missing
  "by user" suffix).
- Error message in `forget` number parser was inconsistent with other commands.

### Added

- **`hooks`**: Added pre/post hook support for `snapshot`, `restore`, `forget`,
  `clean`, and `verify` commands. Hooks are configured in the TOML config file
  and receive `MAPACHE_COMMAND`, `MAPACHE_REPOSITORY`, and `MAPACHE_RESULT`
  environment variables. Optional timeout per hook. Skipped on `--dry-run`.
  Supports `--pre-hook` and `--post-hook` CLI flags to override the
  TOML-configured hooks for a single invocation. Only the hooks for the active
  command are loaded into memory.
- **`bundle --export-snapshot`**: Export a repository snapshot to a
  self-contained `.mapache` bundle file. Requires `-r` / `--repo`.
- **`bundle --import`**: Import a `.mapache` bundle file as a new snapshot
  into the repository. Already-present blobs are skipped automatically
  (cross-repo deduplication). Requires `-r` / `--repo`.
- **`bundle --as-root`**: Use a single directory as the bundle root in bundle
  mode. The directory's children become the top-level items in the bundle,
  matching the behavior of `snapshot --as-root`.

### Changed

- **Incremental restore with `--verify`**: When `--verify` is used, the restorer
  now checks each blob individually against the local file content. Only blobs
  whose content has changed are downloaded and written; unchanged blobs are
  skipped, preserving existing bytes on disk. This significantly speeds up
  repeated restores of large files where only a fraction of the content changed.
- **Restorer**: Ordered metadata restoration as chown, xattrs, chmod, mtime;
  batched file restoration with a streaming pass;verify hardlink content and
  fall back to copy on failure.

## v0.5.2 (2026-07-08)

### Fixed

- Fixed `--exclude` paths in `mapache bundle`.

## v0.5.1 (2026-06-27)

### Fixed

- Fixed help text for init command.

### Added

- Added Linux Android ARM64 target.
- **`copy` command**: Copy snapshots from one repository to another with
  support for `--host`, `--tags`, and `--snapshot` filters.

## v0.5.0 (2026-06-23)

### Added

- **`--stdin` for `snapshot`**: New `--stdin` flag to read backup data from
  stdin as a single virtual file at `/stdin`, with full deduplication and
  encryption.
- **Interruptible `clean`**: The `clean` (and `forget --clean`) command can now
  be interrupted with SIGINT / SIGTERM. The shutdown signal is polled at safe
  checkpoints between GC phases.
- **JSON output for `find`**: The `find` command now supports `--json` output.
- **Error codes for `find` and `mount`**: Both commands now exit with
  meaningful error codes: `10` on repo open failure, `20` on command failure,
  and `130` on interrupt (mount only).
- **Sliding-window rate estimator**: Replaced indicatif's lifetime-average
  `per_sec`/`eta` with a configurable 10s sliding window for responsive ETA
  and throughput in progress bars (CLI and TUI).
- **Recursive filename search**: `find` now searches recursively when the
  pattern does not contain a `/` (e.g. `find file.txt` matches in any
  subdirectory). Use a leading `/` to restrict the search to the root
  (`find /file.txt`).
- **`dump` command**: New `dump` command to print the contents of a single
  file from a snapshot to stdout. Supports snapshot ID prefixes and `latest`.
- **`--no-lock`**: Added an option to open a repository without acquiring a
  lock.
- **Interactive diff screen (TUI)**: New `DiffScreen` with a navigable tree of
  changes (`+`/`~`/`-`), inline expand/collapse, `u` to toggle unchanged files,
  `<`/`>` to browse adjacent snapshot pairs, live spinner while loading, and
  `/` to filter entries by path.
- **Interactive find screen (TUI)**: New `FindScreen` with real‑time glob
  search across all snapshots, progress bar with spinner, results table, inline
  detail panel, and direct navigation to file explorer and restore.
- **Redesigned TUI theme**: The TUI now uses a refined colour palette and
  layout for improved readability.
- **config command**: Added a new `config` command to generate template config
  files.

### Changed

- **Restorer performance**: Optimized the restorer with a flattened parallel
  pipeline, concurrent blob decoding, and JIT file initialization. This
  improves restoration performance while maintaining a low memory footprint.
- **GC memory usage**: Repack memory usage is now bounded by a configurable
  budget, preventing excessive memory consumption during garbage collection
  with large packs.

### Fixes

- **Exit code on interrupt**: `snapshot`, `restore`, `sync`, `verify`, `forget`,
  `rechunk`, `amend`, and `rebuild-index` now all exit with code `130` (the
  conventional `128 + SIGINT`) when the user cancels the operation. Previously
  several of these silently returned success (exit `0`) on interrupt, leaving
  scripts with no way to detect the cancellation.

## v0.4.2 (2026-05-31)

### Security

- **Strict SSH Verification**: Implemented strict host key verification for the
  SFTP backend. The system now verifies server keys against `known_hosts` and
  prompts for confirmation on unknown hosts, preventing MITM attacks.
  Added support for default `known_hosts` locations on Unix and Windows.
- **Memory Safety**: Eliminated technical Undefined Behavior (UB) in
  `SecureStorage` compression by refactoring uninitialized buffer management.
  Maintained performance by avoiding zero-initialization while ensuring
  Rust's safety guarantees.

### Fixes

- **SFTP Backend**: Improved error reporting in the SFTP backend to show the full
  cause chain, making it easier to diagnose authentication and connection
  failures. Fixed a bug where some authentication errors were partially swallowed.
- **S3 Backend**: Fixed a bug where paths in the S3 backend were incorrectly
  joined, potentially bypassing the prefix configuration.

## v0.4.1 (2026-05-31)

### Fixes

- **Restorer Performance**: Fixed a regression introduced in v0.4.0 where
  parallel file writes within pack segments were lost, degrading restore speed.
  Per-file write batching with concurrent flush is now restored, and peak memory
  is further reduced by streaming pack segments and tightening the decoded data
  budget.
- **Progress Bar**: Fixed progress bar never reaching 100% when restoring with
  `--strategy newer` or `--strategy skip`. Skipped bytes are now correctly
  reported as processed.

### Changes

- **Archiver Performance**: Overlap I/O and content-defined chunking with
  compression and encryption for improved snapshot throughput.

## v0.4.0 (2026-05-28)

### Changes

- **Experimental TUI**: Introduced a modular Terminal User Interface for
  interactive repository management. Includes a dashboard, snapshot creation,
  restoration, and retention management. Enable via `--features tui`.
- **Bundle command**: New `bundle` command to create and restore standalone
  `.mapache` bundle files with full deduplication and encryption.
- **FUSE Bundle Mount**: Mount `.mapache` bundles as read-only filesystems
  via the `mount` command.
- **TOML Config File**: Added support for a `.toml` configuration file to
  centralize repository settings and runtime defaults.
- **Enhanced Retention Rules**: Added `--host` (multiple), `--keep-hourly`, and
  `--keep-min` flags to the forget command for more granular snapshot retention
  control.
- **Memory-Efficient Restore**: Blobs are now written immediately after
  decoding, reducing peak memory from O(all blobs in segment) to O(1 blob).
  Cuts restore memory usage without sacrificing parallelism.
- **Robust S3 Backend**: Implemented multipart uploads for files >= 128 MiB
  and recursive listing to avoid hundreds of directory requests during
  garbage collection.
- **Improved GC Efficiency**: Stream snapshots instead of loading all into
  memory upfront and process repack chunks in parallel with pipelining.
- **Memory-Efficient Verification**: Refactored verification to use a streaming
  approach, capping memory usage for large packs.
- **Enhanced Atomic Locks**: Added better metadata and robust stale lock
  detection with detailed conflict reporting.
- **Hardlink Restoration**: Restorer now detects and recreates hardlinks from
  snapshots, preserving inode sharing and nlink counts on Unix.
- **Access Time Preservation**: Added `--with-atime` flag to `snapshot` to
  optionally store and restore file access times. atime is not stored by default
  to avoid unnecessary metadata growth. On Linux, `O_NOATIME` is used when
  reading files to prevent the backup process from modifying access times.
- **Key export**: Added `key export` subcommand to extract a key file from the
  repository and save it locally.

## v0.3.0 (2026-05-05)

### Changes

- **Multi-platform Build System**: Overhauled the build system to support
  static, cross-platform releases for Linux (x64/ARM), Windows, and macOS.
- **Self-contained binary**: Link all dependencies statically for release
- **Redesigned Restorer**: Implemented a new high-performance, pack-centric
  restoration engine with background prefetching, range-based downloads, and
  concurrent restoration to significantly improve I/O efficiency.
- **Improved Metadata Restoration**: File and directory metadata are now
  restored in a separate bottom-up pass to ensure consistency.
- **Environment Variables**: Added support for `MAPACHE_REPOSITORY`,
  `MAPACHE_USERNAME`, and `MAPACHE_PASSWORD` to simplify automation and scripting.
- **Enhanced CLI UI**: Improved error and warning messages with clearer
  formatting and better cross-platform color support.
- Added `--exclude-file` and `--include-file` to read include and exclude paths
  from file.
- **Return codes**: Mapache can now return error codes. Experimental support
  added for the `init`, `snapshot`, `restore`, `verify`, `clean`, `sync`,
  `forget` and `ls` commands.

### Fixes

- Make sure that locks are always released under normal termination.
- Update dependencies patching some vulnerabilities.
- **Security fixes**:
  - Ensure files are never restored outside of the target directory.
  - Zeroize sensitive data (passwords, keys, etc.) after use.
  - Hardened FUSE permissions.
- **Stability and Safety**:
  - Optimized memory usage during restoration by using a fixed-size buffer for
    file verification.
  - Hardened garbage collection and retention logic by replacing risky panics
    with structured error handling.
  - Fixed a bug in `restore` with `--delete` where some nodes were not
    correctly identified for deletion.

## v0.2.3 (2026-04-01)

### Fixes

- Minor optimizations and cosmetic fixes.

### Changes

- Improved `mapache sync`.
- Allow snapshots with paths from multiple logical units (C:, D:, etc.).
- Extended metadata support.
  - Unix extended attributes (xattr).
  - Linux file attributes (`chattr`/`lsattr`).
  - Windows file attributes (Hidden, System, etc.).
  - Improved directory metadata restoration with a bottom-up pass.

## v0.2.2 (2026-03-22)

### Fixes

- `mapache restore` can now be interrupted again.

### Changes

- Added a rate limiter for all backends. The upload and download rates can now
  be selected with the `--limit-upload` and `--limit-download` options.
  For backends that do not support streaming (local and S3), the upload and
  downloads will be done in bursts, with an targeting the limit as an average
  rate. SFTP implements native throttling by sending chunks of dynamic size,
  targeting the limit as an average rate.

## v0.2.1 (2026-03-08)

### Fixes

- The LockHandle will no longer try to refresh or delete a lock in a dry
  backend as there is no lock file to delete, sparing some operations and
  retries in the backend.
- Dry backends no longer try to create the backend.
- Updated aws-lc-sys dependencies solving some security vulnerabilities.
- Disable permission checking in mounted snapshot. This would prevent the user
  from accessing snapshot nodes if the UID and GID differ from the user's.

### Changes

- When `mapache snapshot` cannot open a directory or file, it will print a
  warning but still continue with the snapshot.
- Replaced some sync utility functions with fully async versions.
- Improved scanning of node metadata, making the processing of small files
  a bit faster.

## v0.2.0 (2026-03-02)

### Fixes

- **Critical**: Fixed a bug in `ChunkStream` where it would fail to grow its
  buffer before reading, resulting in sub-optimal chunking and degraded
  performance.
- Fixed a bug that prevented old index files to be deleted by `mapache
  rebuild-index`.
- Cancel the scanning thread when `mapache snapshot` has finalized the tree in
  the rare case that the snapshot process is faster than the scanner.
- Removed unnecessary directory listing when parsing a pack footer.
- Fixed a bug that created the repository backend root before authenticating
  the user in the init command.

### Changes

- **Async Refactor**: The entire core has been refactored to use `tokio` for
  asynchronous I/O and concurrency. This improves performance and provides
  better resource management under high load.
- **S3 Backend**: Initial support for S3-compatible storage backends using
  `rust-s3`.
- **SFTP Backend**: Reimplemented the SFTP backend using `russh`. The new
  implementation is fully async, more reliable, and supports connection pooling
  with backpressure.
- **Memory Optimizations**: Heavy optimizations to the archiver and packer
  pipelines to bound memory usage and implement zero-copy buffer recycling.
- **Performance**: Optimized `LocalFS` to use `read_buf` for efficient, safe I/O
  without zero-initialization overhead where possible.
- The FSNodeStream now stats children in parallel.
- Added more stats to `mapache stats` and support for json output.
- Enhanced Repository Verification:
  - Bit-rot detection via full-file hashing of pack files.
  - Index-to-pack consistency checks to ensure all indexed blobs physically
    exist.

## v0.1.8 (2026-02-05)

### Changes

- `mapache verify` now doesn't use the local cache by default. Added a
  `--with-cache` option to enable it.
- Optimized `snapshot` performance by implementing `Packer` buffer recycling and
  offloading packer flushing to the `PackSaver` threads.
- Mapache can now print json output for selected commands using the`--json`
  option. Initial support is still limited to `init`, `log`, `snapshot` and
  `forget` commands.
- The repository master index is now only loaded when necessary, saving some
  RAM for those commands that don't need to access the index.

## v0.1.7 (2026-01-29)

### Fixes

- `mapache verify` is now very explicit about data corruption in the pack files.
  Before, it would log an error when a corrupt pack is found, but the logical
  check (indexed refs) could be `[OK]` and the user would be misled that the
  verification passed.
- Fixed an inconsistency in the number of reported processed items in
  incremental snapshots.

### Changes

- `--read-concurency` and `--write-concurrency` in `mapache snapshot` are now
  called `--readers` and `--packers`.
- `mapache find` now accepts patterns and allows finding in a selected snapshot.
- Minor changes to the snapshot report.

## v0.1.6 (2026-01-26)

### Changes

- Improved error messages.
- Better progress logs in `mapache sync`.
- Unified the aggregator and pack saver stages in the Archiver pipeline for
  performance.
- Throttle UI events to improve responsiveness.

### Fixes

- Fixed snapshot summary data missing in the snapshot metadata.
- Fixed abbreviation of Windows paths appending a redundant separator after the
  root prefix.

## v0.1.5 (2026-01-24)

### Changes

- Added an aggregator stage to the Archiver pipeline to receive blobs from the
  processor threads and pack them in parallel.
- Big optimizations in the Archiver pipeline buffering to promote Zero-Copy for
  massive gains in performance. Mapache has traditionally been slow when
  processing many small files. This weakness seems to have been eliminated.
- The compression level is now configurable. Added a `--compression` global
  option to control the compression level.
- `--include` and `--exclude` now accept patterns. For example,
  `--exclude **/*.jpg` would exclude all .jpg files in every folder.
- Using the `mimalloc` allocator on Windows.
- `mapache cache` now displays the total size of the local cache.

## v0.1.4 (2026-01-16)

### Changes

- Optimized the `verify` command massively. Removed unnecessary duplicate checks
  and parallelize verification of packs.

### Fixes

- Fixed size reported by `rebuild-index`.

## v0.1.3 (2026-01-15)

### Changes

- Concurrency: Added a global `--retry-lock` to setup a timeout to retry
  acquiring a lock if the repository is already locked. No retries are attempted
  by default.
- Disaster Recovery: Added a `rebuild-index` command to rebuild the index by
  scanning all existing packs.
- Memory Optimization: The index `reverse_map` is now ephemeral. It is only
  loaded into RAM when strictly necessary (e.g., during clean or rebuild-index),
  drastically reducing the memory footprint for large repositories.

## v0.1.2 (2026-01-11)

### Changes

- Added a `find` command to find files and directories in the repository.
- Added a `--metadata-only` to `mapache mount` to mount snapshots with metadata
  only (no file data).
- Added an option to `mapache mount` to select the amount of memory allocated to
  the internal data cache.
- Highlight the directory path in recursive `ls`.
- Display a [DRY RUN] flag in snapshot and restore for better context.
- Optimizations to the index affecting the garbage collector.
- Optimizations to the FUSE internals.

## v0.1.1 (2025-12-18)

### Changes

- Added a `--no-preserve-root` option to `mapache restore`.
  This option is only used together with `--delete`. By default, `--delete` does
  not delete any node in the root directory as a protection. `--no-preserve-root`
  explicitly overrides this protection.
- Added a `--no-repack` option to `mapache clean` to disable repacking during
  garbage collection. Internally, this is equivalent to setting the tolerance
  to 100 %.

### Fixes

- Read-only files can now be renamed and deleted in Windows 10.

## v0.1.0 (2025-11-27)

### Changes

- Added a `rechunk` command to reprocess all files in all snapshot and rechunk
  them with the current chunker and parameters. The snapshots are rewritten,
  but the old chunks will be left unreferenced and deleted by the next run on
  the garbage collector.
- The custom chunker is now the default and only chunker. Use the `rechunk`
  command to run the deduplication on older repositories. The old chunks will
  be removed by the garbage collector the next time `clean` is called.
- The period-based retention rules (`keep-daily`, `keep-weekly`, `keep-monthly`
  and `keep-yearly`) now keep one snapshot per period for the last N periods
  instead of the last N snapshots.
- Added a `--skip-if-unchanged` flag to `mapache snapshot` to skip saving
  snapshots without new changes.

## v0.1.0-beta.5 (2025-11-14)

### Changes

- Abort snapshot early if a fatal error occurs.
- Run file system scan concurrently with the snapshot task.
- Added a new experimental chunker. This chunker is not used by default. To
  enable the new chunker, build mapache with the `custom-chunker` feature.

### Fixes

- Failing to read a symlink's target is not an error. Mapache stores all the
  metadata it can and continue.

## v0.1.0-beta.4 (2025-11-01)

### Changes

- Delete all .tmp files from the repository during GC.
- Best effort metadata restoring. Failing to restore metadata is not an error,
  only a warning.
  Highly likely and recurrent warnings, which don't affect the integrity of the
  data, should not be logged.
- `mapache forget` does not delete the snapshots permanently unless `--force` is
  used. The 'forgotten' snapshots are
  only marked for deletion (dropped) for the garbage collector. This prevents
  accidental deletion.
- Added a `recall` command to recover 'forgotten' (dropped) snapshots. `mapache
  log` can now also list dropped snapshots.

### Fixes

- Fixed calculation of file hashes (regression). The hash of a file is
  calculated after the contents are (potentially) encoded.
- Ignore files with invalid ID names when loading indices, snapshots and keys.
- Enforce read concurrency in snapshot cmd.
- Fixed unclear cleanup handler logs in snapshot cmd.

## v0.1.0-beta.3a (2025-10-27)

### Changes

- Support ssh keys for dst backend in `cmd_sync`.
- Minor optimizations to the Archiver.
- `cat` command now accepts a prefix ID for blobs and trees.
- Added `completion` command to generate autocompletion scripts.
- Restore symlink metadata on Unix.
- Added ETA to sync progress bar.
- Implemented a local cache to speedup reading frequently used metadata and
  reduce download operations.
- Added a `cache` command to manage cache folders.

## v0.1.0-beta.2a (2025-10-12)

### Changes

- Don't emit a warning for the first snapshot.
- Made FUSE a feature. The `mount` command can be disabled during compilation on
  those systems that don't support it.
- `clean` will now de-duplicate blobs existing in different packs.
- Minor optimizations to the index and garbage collector
- Minor optimizations to the SecureStorage and archiver pipeline.
- UI refresh rate is now configurable using the $MAPACHE_REFRESH_RATE
  environment variable and set to 10 Hz by default.
- Delete expired logs when acquiring a new lock. Implemented try-and-check to
  detect conflicts.

### Fixes

- Fail if no source paths are provided as arguments for `cmd_snapshots`.
- Append ID to snapshot folder in by_date directory in the FUSE mount to
  distinguish snapshots with close timestamps.

### Others

- Added many new tests, which helped find some bugs.

## v0.1.0-beta.1 (2025-09-28)

### Fixes

- Create restore target only once.
- Don't fail if directory was already restored.

## v0.1.0-alpha.5 (2025-09-15)

### Enhancements

- Added percentage indicator to snapshot, restore and clean.
- Improved progress reporting during snapshot verification.
- Remove mountpoint if `cmd_mount -c` created it.
- Added username to authentication. Accessing the repository now requires a
  username and password.
- Added a key command to manage Keyfiles. Keys can be created, modified and
  deleted.
- Added a sync command to synchronize a repository in a different backend.

### Changes

- `--resolution` option in `cmd_restore` is now called `--strategy`

### Fixes

- Report skipped nodes when restoring (increment processed items and bytes
  counters).
- Don't follow symlinks when normalizing source paths.
- Create parent directories before restoring symlinks

## v0.1.0-alpha.4 (2025-08-29)

- Added a progress spinner to log stats progress while analyzing snapshots.
- Added cross-compile support to compile builds for x64 Linux and Windows.

## v0.1.0-alpha.3 (2025-08-15)

- Diverse cleanups and micro-optimizations
- Cleaner report of restore warnings and errors
- Fix 'latest' symlink in FUSE mount

## v0.1.0-alpha.2 (2025-08-13)

- Fix cmd_mount not writing lock file
- Repack small packs based on the current pack size value
- Updated dependencies

## v0.1.0-alpha.1 (2025-08-07)

This is the first pre-release version of mapache.

The repository format cannot be considered stable and final yet. mapache has a
considerable number of features now, but I need to shift the focus on stability
and testing. Some planned features are missing and should be implemented in the
coming pre-release versions. The real challenge is deciding when to declare a
repository format as stable and what kind of promises can be made about
backwards compatibility in the future.

Proper documentation is also missing.
