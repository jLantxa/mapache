# Changelog

## Latest
### Enhancements
- Added percentage indicator to snapshot and restore.
- Improved progress reporting during snapshot verification.
- Remove mountpoint if cmd_mount -c created it.
- Added username to authentication. Accessing the repository now requires a username and password.
- Added key command to manage Keyfiles. Keys can be created, modified and deleted.

### Fixes
- Report skipped nodes when restoring (increment processed items and bytes counters).

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

**This is an early development alpha release. It should be OK to play with it, but don't use mapache at this stage as your main or sole backup tool, especially if you need high reliability, at least without having tested it for a while.**

The repository format cannot be considered stable and final yet. mapache has a considerable number of features now, but I need to shift the focus on stability and testing. Some planned features are missing and should be implemented in the coming pre-release versions. The real challenge is deciding when to declare a repository format as stable and what kind of promises can be made about backwards compatibility in the future.

Proper documentation is also missing.
