# backup

A **work-in-progress** de-duplicating incremental **backup** tool written in Rust.

**Note:**
This software is still a work in progress. The format of the repository is unstable and subject to change, which could render different versions incompatible. It has not been thoroughly tested. For the time being, you should not use this tool for anything important.

---

## Table of Contents

- [About](#about)
- [Roadmap](#roadmap)
- [Getting Started](#getting-started)

---

## About

`[[backup]]` *–placeholder name for the tool (I don't have a name yet)–* is a de-duplicating incremental backup tool written in Rust. It is a CLI tool to back up your data to a local file system or a remote machine. I started this project because the previous backup tool I was using no longer met my needs. I decided to create my own tool and learn something in the process.

`[[backup]]` is still in an early development stage. As such, it is still barely functional, it is missing a lot of features, it is unstable, etc. But more importantly, it is a personal project and a tool I'm making to cover my own backup needs.

The language of choice is `Rust`. I didn't choose `Rust` for any particular reason other than it is a language I'm learning now and it seemed sufficiently safe, performant, and ergonomic to use it.

`[[backup]]` is inspired in its design by other similar tools like `git`, [`BorgBackup`](https://www.borgbackup.org/) and [`restic`](https://restic.net/). It implements a content-addressable repository to store and retrieve binary objects and `content-defined chunking` to de-duplicate the contents of files. It uses the FastCDC algorithm for chunking. Each 'backup' is saved as a `Snapshot`. `Snapshots` are independent of each other and they describe what was backed up and when. Although the `snapshots` are independent, every new `snapshot` only appends the new information that was different from the already existing `snapshots`.

To provide data protection, all data stored in the repository is encrypted and authenticated using 256-bit AES-GCM, with Argon2 for key derivation.

### Guiding Principles

The development of `[[backup]]` is guided by the following core principles:

-   **Generality**: The tool should function effectively across various contexts, from small to large repositories and diverse machine specifications.

-   **Efficiency**: It must use host resources optimally, completing backups quickly without exhaustion and minimizing storage footprint.

-   **Robustness**: The tool needs to resume operations seamlessly after interruptions, ensuring repository integrity and data reliability.

-   **Security**: All data in the repository must be encrypted and authenticated.

-   **Self-Containment**: I'm aiming for `[[backup]]` to be entirely self-contained, with all dependencies statically linked. Even if this means longer compilation times and a larger executable, it offers the significant benefit of being executable from a USB stick on a fresh installation without an internet connection in a hard time.


## Roadmap

`[[backup]]` is still in early development. I have two milestones currently planned.

### `Snapshots` milestone *(complete)*

The first `Snapshots` milestone consists of implementing the core architecture and a minimal set of functional features. This includes:

-   [x] Creating `snapshots` (`Archiver` pipeline).
-   [x] Restoring `snapshots` (`Restorer` pipeline).
-   [x] Listing `snapshots`.
-   [x] Local and SFTP backends.

**This milestone is complete.**

### `Garbage Collection` milestone

The second milestone consists of adding features related to repository maintenance and garbage collection and other convenience options. This includes:

-   [x] Removing snapshots, with basic retention rules. The `forget` command.
-   [x] `gc` command. A command to remove obsolete objects from the repository that are not referenced by any snapshot.
-   [x] Run `gc` optionally in `forget` command. Run `gc` after `forget` for convenience.
-   [x] `--exclude` filter in `snapshot` command.
-   [x] `--include` / `--exclude` in `restore` command.
-   [x] `ls` command to list paths in a snapshot.

After that, the plan is to expand the functionality with new options, features, optimizations, and ergonomics. Choosing a name for the tool is also something that should happen at some time –I hope–.

### Other planned features

This is a non-exhaustive list of features that I want to add:

-   [ ] `ssh` authentication with public key for the `sftp` backend.
-   [ ] FUSE (I don't even know how this works).

### Others

Right now, I am working on a functional prototype that works so I can start doing backups ASAP. Even though I am trying to develop with optimizations in mind, I am aware that a better job can always be done. Better optimizations are a job for future me.

## Getting started

### Building with `cargo`
To compile `[[backup]]` from source you just need to install `Rust` on your machine and build it with cargo:

```
cargo build

# Or, for an optimized and faster executable:
cargo build --release
```

You need to install `perl` in our system in order to compile the `openssl` sources.

### Running
If you run the executable, you will be greeted by something like this:

```
[backup] is a de-duplicating, incremental backup tool

Usage: backup [OPTIONS] --repo <REPO> <COMMAND>

Commands:
  init      Initialize a new repository
  snapshot  Create a new snapshot
  restore   Restore a snapshot
  log       Show all snapshots present in the repository
  forget    Remove snapshots from the repository
  ls        List nodes in the repository
  cat       Print repository objects
  help      Print this message or the help of the given subcommand(s)

Options:
  -r, --repo <REPO>                    Repository path
  -p, --password-file <PASSWORD_FILE>  Path to a file to read the repository password
  -k, --key <KEY>                      Path to a KeyFile
  -q, --quiet
  -v, --verbosity <VERBOSITY>
  -h, --help                           Print help
  -V, --version                        Print version
```
