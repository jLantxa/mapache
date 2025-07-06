# mapache

A **work-in-progress** de-duplicating incremental **backup** tool written in Rust.

**Note:**
This software is still a work in progress. The format of the repository is unstable and subject to change, which could render different versions incompatible. It has not been thoroughly tested. For the time being, you should not use this tool for anything important.

<img src="doc/res/mapache.png" alt="mapache logo" width="200"/>

---

## Table of Contents

- [About](#about)
- [Roadmap](#roadmap)
- [Getting Started](#getting-started)

---

## About

`mapache` is a de-duplicating incremental backup tool written in Rust. It is a CLI tool to back up your data to a local file system or a remote machine. I started this project because the previous backup tool I was using no longer met my needs. I decided to create my own tool and learn something in the process. It is still in an early development stage. As such, it is still missing a lot of features, it is unstable, etc.

`mapache` is writen in `Rust`. I didn't choose `Rust` for any particular reason. Other languages like Go or C++ could have been a good choice too. For a tool like this, the language is not so important.

`mapache` is inspired in its design by other similar tools like `git` and [`restic`](https://restic.net/). It implements a content-addressable repository to store and retrieve binary objects and `content-defined chunking` to de-duplicate the contents of files. It uses the v2020 FastCDC algorithm for chunking and deduplication. Each 'backup' is saved as a `Snapshot`. `Snapshots` are independent of each other and they describe the status of your filesystem when you did the backup (files, directories and their metadata). Although the `snapshots` are independent, every new `snapshot` only appends the new information that was different from the already existing `snapshots`.

To provide data protection, all data stored in the repository are encrypted and authenticated using 256-bit AES-GCM, with Argon2 for key derivation.

### Guiding Principles

The development of `mapache` is guided by the following core principles:

-   **Generality**: The tool should function effectively across various contexts, from small to large repositories and diverse machine specifications.

-   **Efficiency**: It must use host resources optimally, completing backups quickly without exhaustion and minimizing storage footprint.

-   **Robustness**: The tool needs to resume operations seamlessly after interruptions, ensuring repository integrity and data reliability.

-   **Security**: All data in the repository must be encrypted and authenticated. No one but you should be able to access the data even if others get access to the storage medium.

-   **Self-Containment**: I'm aiming for `mapache` to be entirely self-contained, with all dependencies statically linked. Even if this means longer compilation times and a larger executable, it offers the significant benefit of being executable from a USB stick on a fresh installation without an internet connection in a hard time. This is a soft requirement that could be lost in favour of the others.


## Roadmap

`mapache` is still in early development going through milestones.

### 1. `Snapshots` *(complete)*

The first `Snapshots` milestone consists of implementing the core architecture and a minimal set of functional features. This includes:

- [x] Creating `snapshots` (`Archiver` pipeline).
- [x] Restoring `snapshots` (`Restorer` pipeline).
- [x] Listing `snapshots`.
- [x] Local and SFTP backends.

### 2. `Garbage Collection` *(complete)*

The second milestone consists of adding features related to repository maintenance and garbage collection and other convenience options. This includes:

- [x] Removing snapshots, with basic retention rules. The `forget` command.
- [x] `gc` command. A command to remove obsolete objects from the repository that are not referenced by any snapshot.
- [x] Run `gc` optionally in `forget` command. Run `gc` after `forget` for convenience.
- [x] `--exclude` filter in `snapshot` command.
- [x] `--include` / `--exclude` in `restore` command.
- [x] `ls` command to list paths in a snapshot.
- [x] `ssh` authentication with public key for the `sftp` backend.

After that, the plan is to expand the functionality with new options, features, optimizations, and ergonomics. Choosing a name for the tool is also something that should happen at some time –I hope–.

### 3. *`Smooth mapache`*

The goal of this milestone is to add convenience and quality of like feature. Things that are not strictly necessary but make mapache nicer to use. The real goal is to work towards a stable repository format that allows me to add features in the future without making previous versions incompatible. This includes:

- [x] `amend` command to remove files from existing snapshots and modify metadata.
- [ ] `diff` command to show differences between snapshots
- [ ] `verify` command to verify the integrity of the data stored in the repository.
- [ ] Key managment.

### Other planned features

This is a non-exhaustive list of features that I want to add:

- [ ] FUSE mount (I don't even know how this works).

## Getting started

### Building with `cargo`
To compile `mapache` from source you just need to install `Rust` on your machine and build it with cargo:

```
cargo build

# Or, for an optimized and faster executable:
cargo build --release
```

You need to install `perl` in our system in order to compile the `openssl` sources.

### Running
If you run the executable, you will be greeted by something like this:

```
mapache is a de-duplicating, incremental backup tool

Usage: mapache [OPTIONS] --repo <REPO> <COMMAND>

Commands:
  init      Initialize a new repository
  snapshot  Create a new snapshot
  restore   Restore a snapshot
  log       Show all snapshots present in the repository
  amend     Amend an existing snapshot
  forget    Remove snapshots from the repository
  gc        Remove obsolete objects from the repository
  ls        List nodes in the repository
  diff      Show differences between snapshots
  cat       Print repository objects
  help      Print this message or the help of the given subcommand(s)

Options:
  -r, --repo <REPO>                      Repository path
      --ssh-pubkey <SSH_PUBKEY>          SSH public key
      --ssh-privatekey <SSH_PRIVATEKEY>  SSH private key
  -p, --password-file <PASSWORD_FILE>    Path to a file to read the repository password
  -k, --key <KEY>                        Path to a KeyFile
  -q, --quiet
  -v, --verbosity <VERBOSITY>
  -h, --help                             Print help
  -V, --version                          Print version
```
