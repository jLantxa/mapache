# Changelog

## Latest

### Fixes

- Minor optimizations and cosmetic fixes.

### Changes

- Improved `mapache sync`.

## v0.2.2

### Fixes

- `mapache restore` can now be interrupted again.

### Changes

- Added a rate limiter for all backends. The upload and download rates can now
  be selected with the `--limit-upload` and `--limit-download` options.
  For backends that do not support streaming (local and S3), the upload and
  downloads will be done in bursts, with an targeting the limit as an average
  rate. SFTP implements native throttling by sending chunks of dynamic size,
  targeting the limit as an average rate.

## v0.2.1

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

## v0.2.0

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

## v0.1.8

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

## v0.1.7

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

## v0.1.6

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

## v0.1.5

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

## v0.1.4

### Changes

- Optimized the `verify` command massively. Removed unnecessary duplicate checks
  and parallelize verification of packs.

### Fixes

- Fixed size reported by `rebuild-index`.

## v0.1.3

### Changes

- Concurrency: Added a global `--retry-lock` to setup a timeout to retry
  acquiring a lock if the repository is already locked. No retries are attempted
  by default.
- Disaster Recovery: Added a `rebuild-index` command to rebuild the index by
  scanning all existing packs.
- Memory Optimization: The index `reverse_map` is now ephemeral. It is only
  loaded into RAM when strictly necessary (e.g., during clean or rebuild-index),
  drastically reducing the memory footprint for large repositories.

## v0.1.2

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

## v0.1.1

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

## v0.1.0

### Changes

- Added a `rechunk` command to reprocess all files in all snapshot and rechunk
  them with the current chunker and parameters. The snapshots are rewriten,
  but the old chunks will be left unreferenced and deleted by the next run on
  the garbage collector.
- The custom chunker is now the default and only chunker. Use the `rechunk`
  command to run the deduplication on older repositories. The old chunks will
  be removed by the garbage collector the next time `clean` is called.
- The period-based retention rules (`keep-daily`, `keep-weekly`, `keep-monthly`
  and `keey-yearly`) now keep one snapshot per period for the last N periods
  instead of the last N snapshots.
- Added a `--skip-if-unchanged` flag to `mapache snapshot` to skip saving
  snapshots without new changes.

## v0.1.0-beta.5

### Changes

- Abort snapshot early if a fatal error occurrs.
- Run file system scan concurrently with the snapshot task.
- Added a new experimental chunker. This chunker is not used by default. To
  enable the new chunker, build mapache with the `custom-chunker` feature.

### Fixes

- Failing to read a symlink's target is not an error. Mapache stores all the
  metadata it can and continue.

## v0.1.0-beta.4

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

## v0.1.0-beta.3a

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

## v0.1.0-beta.2a

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

## v0.1.0-beta.1

### Fixes

- Create restore target only once.
- Don't fail if directory was already restored.

## v0.1.0-alpha.5

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

## v0.1.0-alpha.4

- Added a progress spinner to log stats progress while analyzing snapshots.
- Added cross-compile support to compile builds for x64 Linux and Windows.

## v0.1.0-alpha.3

- Diverse cleanups and micro-optimizations
- Cleaner report of restore warnings and errors
- Fix 'latest' symlink in FUSE mount

## v0.1.0-alpha.2

- Fix cmd_mount not writing lock file
- Repack small packs based on the current pack size value
- Updated dependencies

## v0.1.0-alpha.1

This is the first pre-release version of mapache.

The repository format cannot be considered stable and final yet. mapache has a
considerable number of features now, but I need to shift the focus on stability
and testing. Some planned features are missing and should be implemented in the
coming pre-release versions. The real challenge is deciding when to declare a
repository format as stable and what kind of promises can be made about
backwards compatibility in the future.

Proper documentation is also missing.
