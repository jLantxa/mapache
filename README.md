# mapache

![Badge](https://github.com/jlantxa/mapache/workflows/main/badge.svg)

Mapache is a **fast, secure, de-duplicating, incremental **backup** tool**
written in Rust.

You can find more [in-depth documentation](doc/mapache.md).

---

## Table of Contents

- [About](#about)
- [Getting started](#getting-started)
- [Roadmap](#roadmap)

---

## About

`mapache` is a program that helps you to make backups of your data. It is
designed to be fast, efficient and secure.

`mapache` is inspired in its design by other similar tools like `git` and
[`restic`](https://restic.net/). It implements a content-addressable repository
to store and retrieve binary objects and `content-defined chunking` to
de-duplicate the contents of files. It uses a custom implementation of the
FastCDC algorithm for chunking and de-duplication. Each 'backup' is saved as a
`Snapshot`. `Snapshots` are independent of each other and they describe the
status of your file system when you did the backup (files, directories and their
metadata). Although the `snapshots` are independent, every new `snapshot` only
appends the new information that was different from the already existing
`snapshots`.

To provide data protection, all data stored in the repository are encrypted and
authenticated using 256-bit AES-GCM-SIV, with Argon2 for key derivation.
Encryption is non-negotiable and cannot be disabled.

### Guiding Principles

The development of `mapache` is guided by the following core principles:

- **Generality**: The tool should function effectively across various contexts,
  from small to large repositories and diverse machine specifications.
- **Efficiency**: It must use host resources optimally, completing backups
  quickly without exhaustion and minimizing storage footprint.
- **Robustness**: The tool needs to resume operations seamlessly after
  interruptions, ensuring repository integrity and data reliability.
- **Security**: All data in the repository must be encrypted and authenticated.
  No one but you should be able to access the data even if others get access to
  the storage medium.
- **Self-Containment**: I'm aiming for `mapache` to be entirely self-contained,
  with all dependencies statically linked. Even if this means longer compilation
  times and a larger executable, it offers the significant benefit of being
  executable from a USB stick on a fresh installation without an internet
  connection in a hard time. This is a soft requirement that could be lost in
  favour of the others.

## Roadmap

### v0.1.0 (_we are here_)

mapache 0.1.0 is the first public stable release. It is meant to be a first
stable prototype with all core features after 8 months of work.

The repository format can suffer small changes in the next few versions as it
converges to a final solution. The archiver pipeline (backup) is already very
efficient, but the restore needs a redesign to optimize the performance and
minimize I/O operations.

### v0.2.0

After the Christmas season, I will resume implementing all features that didn't
make it into v0.1.0. At the moment, these include:

- [ ] configuration files,
- [ ] master key rotation,
- [ ] `restore` redesign,
- [ ] reimplement SFTP backend with a pure rust crate,
- [ ] return codes for commands,
- [x] regex filters,

and other internal refactors, optimizations and bug fixing.

## Getting Started

### Building mapache

To compile `mapache` from source you just need to install `Rust` on and build
with cargo:

```bash
cargo build

# Or, for an optimized and faster executable:
cargo build --release
```

### Dependencies

- You need to install `perl` in your system in order to compile the `openssl`
  sources.
- Some systems require a development version of the fuse library. FUSE, which is
  used for the `mount` command, is only available on Unix-like systems.
  To build mapache without fuse support, use the `--no-default-features` when
  building.

```bash
cargo build --release --no-default-features
```

### Running

If you run the executable, you will be greeted by something like this:

```text
mapache backup tool

Usage: mapache <COMMAND>

Commands:
  amend          Amend an existing snapshot
  cache          List and cleanup cache directories
  cat            Print repository objects
  clean          Clean up the repository
  completion     Generate autocompletion scripts
  diff           Show differences between snapshots
  find           Find files and directories in the repository
  forget         Remove snapshots from the repository
  init           Initialize a new repository
  key            Create and manage keys
  log            Show all snapshots present in the repository
  ls             List nodes in the repository
  mount          Mount the repository as a file system
  rebuild-index  Rebuild the index by scanning all existing packs
  recall         Recall forgotten snapshots
  rechunk        Rechunk all snapshots
  restore        Restore a snapshot in a target path
  snapshot       Create a new snapshot
  stats          Display stats about the repository and its contents
  sync           Synchronize a repository in a different location
  unlock         Remove existing locks
  verify         Verify the integrity of the data stored in the repository
  help           Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

You can use the `-h` or `--help` option to show help for every command.
