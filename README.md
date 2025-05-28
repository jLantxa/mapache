# backup

A <u>**work-in-progress**</u> de-duplicating incremental **backup** tool written in Rust.

## Table of Contents

- [About](#about)
- [Roadmap](#roadmap)

## About
`[[backup]]` *-placeholder name for the tool (I don't have a name yet)-* is a de-duplicating incremental backup tool written in Rust. It is a CLI tool to backup your data to a local file system or a remote machine. This project was born because the previous backup tool that I used stopped fulfilling my needs, so I though I'd make my own and also learn something in the process.

`[[backup]]` is still in an early development stage. As such, it is still not functional (only for testing and development), it is missing all sorts of features, it is unstable, etc. But more importantly, it is a personal project and a tool I'm making to cover my own backup needs.

The language of choice is `Rust`. I didn't choose `Rust` for any particular reason other than: it is a language I'm learning now and it seemed sufficiently safe, performant and ergonomic to use it.

`[[backup]]` is inspired in its design by other similar tools like `git`, `BorgBackup` and `restic`. It implements a content-addressable repository to store and retrieve binary objects and `content-defined chunking` to de-duplicate the contents of files. It uses the FastCDC algorithm for chunking. Each 'backup' is saved as a `Snapshot`. `Snapshots` are independent from each other and they describe what was backed up and when. Although the `snapshots` are independent, every new `snapshot` only appends the new information that was different from the already existing `snapshots`.

To provide data protection, all data stored in the repository is encrypted and authenticated using 256-bit AES-GCM, with Argon2 for key derivation.

### Guiding Principles

The development of `[[backup]]` is guided by the following core principles:

- **Generality**: The tool should function effectively across various contexts, from small to large repositories and diverse machine specifications.

- **Efficiency**: It must use host resources optimally, completing backups quickly without exhaustion and minimizing storage footprint.

- **Robustness**: The tool needs to resume operations seamlessly after interruptions, ensuring repository integrity and data reliability.

- **Security**: All data in the repository must be encrypted and authenticated.

- **Self-Containment**: I'm aiming for `[[backup]]` to be entirely self-contained, with all dependencies statically linked. Even if this means a longer compilation times and a larger executable, it offers the significant benefit of being executable from a USB stick on a fresh installation without an internet connection in a hard time.

## Roadmap
`[[backup]]` is still in early development. I have two milestones currently planned.

The first *`Snapshots`* milestone consists of implementing the core architecture and a minimal set of functional features. This includes:

- [x] Creating `snapshots` (`Archiver` pipeline).
- [ ] Restoring `snapshots` (`Restorer` pipeline).
- [x] Listing `snapshots`.
- [x] Local and SFTP backends.

The second *`Garbage collection`* milestone consists of adding features related to repository maintenance and garbage collection. This includes:

- [x] Removing `snapshots`, with basic retention rules.
- [ ] Garbage collection (pruning, or, removing unused and obsolete objects).

After that, the plan is to expand the functionality with new options, features and ergonomics. Choosing a name for the tool is also somethign that should happen at some time --I hope--.

## Getting started

### Building with `cargo`
To compile `[[backup]]` from source you just need to install `Rust` on your machine and build it with cargo:

```
cargo build

# Or, for an optimized and faster executable:
cargo build --release
```

### Running
If you run the executable, you will be greeted by something like this:

```
[backup] is a de-duplicating, incremental backup tool

Usage: backup --repo <REPO> <COMMAND>

Commands:
  init      Initializes a new repository
  snapshot  Creates a new snapshot
  restore   Restores a snapshot
  log       Shows all snapshots present in the repository
  forget    Removes snapshots from the repository
  cat       Prints repository objects
  help      Print this message or the help of the given subcommand(s)

Options:
  -r, --repo <REPO>  Repository path
  -h, --help         Print help
  -V, --version      Print version

```

Each command is independent. For example, if you want to know what options are available for the `snapshot` command, you can do:

```
backup snapshot -h
```

and you will be shown the help for that command:

```
Creates a new snapshot

Usage: backup --repo <REPO> snapshot [OPTIONS] <PATHS>...

Arguments:
  <PATHS>...  List of paths to backup

Options:
      --description <DESCRIPTION>  Snapshot description
      --full-scan                  Force a complete analysis of all files and directories
      --parent <PARENT>            Use a snapshot as parent. This snapshot will be the base when analyzing differences [default: latest]
      --dry-run                    Dry run
  -h, --help                       Print help
```

### Specifying a repo path and initializing a `repository`

`[[backup]]` stores all the data in a `Repository`. This `repository` can be store on the same machine that you used to run the tool, an external harddrive, or a machine accessible via SFTP.

If you want to initialize a repository in the local file system (your machine or a device physically connected to it), you do it like this:

```
backup --repo path/to/repo init
# or
backup --repo file://path/to/repo init
```

If you want to initialize a repository in a remote machine accessible via SSH which supports the SFTP protocol, you use a standardized url string like this:

```
backup --repo sftp://user@host/path/to/repo init
```

The path comes right after `user@host/`, so `sftp://user@host/home` and `sftp://user@host//home` are `home` and `/home` respectively. If no port is specified, it defaults to port 22. To specify a port, do it in a standard way like:

```
backup --repo sftp://user@host:2222/path/to/repo init
```

### Creating a `snapshot`
To do.

### Restoring a `snapshot`
To do.

### Logging your `snapshots`
To do.

### Forgetting a `snapshot`
To do.
