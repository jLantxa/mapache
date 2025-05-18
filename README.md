# backup

A <u>**work-in-progress**</u> deduplicating incremental **backup** tool written in Rust.

## Table of Contents

- [About](#about)
- [Roadmap](#roadmap)

## About
`{{backup}}` *-placeholder name for the tool (I don't have a name yet)-* is a deduplicating incremental backup tool written in Rust. It is a CLI tool to backup your data to a local file system or a remote machine.

`{{backup}}` is still in an early development stage. As such, it is still not functional (only for testing and development), it is missing all sorts of features, it is unstable, etc. But more importantly, it is a learning project and a tool I'm making to cover my own backup needs.

The language of choice is `Rust`. I didn't choose `Rust` for any particular reason other than: it is a language I'm learning now and it seemed sufficiently safe, performant and ergonomic to use it.

`{{backup}}` is inspired in its design by other similar tools like `git` and `restic`. Is implements a content-addressable repository to store and retrieve binary objects and `content-defined chunking` to deduplicate the contents of files.

Each 'backup' is called a `Snapshot`. `Snapshots` are independent from each other and they describe what was backed up and when. Although the `snapshots` are independent, every new `snapshot` only appends the new information that was different from the already existing `snapshots`.

The basic design principles of {{backup}} are:

- **Generality**: The tool must be able to work in a variety of contexts, i.e. small to medium to big repositories, machines of all different specs, etc.
- **Efficiency**: The tool must use the resources available in the host as efficiently as possible. This means completing the backup process as fast as the resources allow, without exhausting those resources and with the minimum storage footprint.
- **Robustness**: The tool must be able to resume operation if interrupted without the repository being corrupted and guaranteeing the integrity of the data.
- **Security**: The tool must provide confidentiality and authentification of the stored data with encryption.

In addition to that, I am aiming to make the tool self contained, with all its dependencies linked statically. Even if this means making the executable bigger, I find it extremely useful that I am able to run it from an USB stick in a fresh install with no network connection.

## Roadmap
{{backup}} is still in early development.

The first milestone consists of implementing the core architecture and a minimal set of functional features. This includes:

- Creating `snapshots`.
- Restoring `snapshots`.
- Listing `snapshots`.

For the time being, only local and SFTP backends are included. That will be **`v0.1.0`**.

The second milestone consists of adding features related to repository maintenance and garbage collection. This includes:

- Removing `snapshots`, possibly with a retention policy.
- Garbage collection (removing unused and obsolete objects).

That should be **`v0.2.0`**.

After that, the plan is to expand the functionality with new options, features and ergonomics.

## Getting started

### Building with `cargo`
To compile {{backup}} from source you just need to install `Rust` on your machine and build it with cargo:

```
cargo build

# Or, for an optimized and faster executable:
cargo build --release
```

### Running
If you run the executable, you will be greeted by something like this:

```
Incremental backup tool

Usage: backup --repo <REPO> <COMMAND>

Commands:
  init      Initialize a new repository
  log       Show all snapshots present in the repository
  snapshot  Create a new snapshot
  restore   Restores a snapshot
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
Create a new snapshot

Usage: backup --repo <REPO> snapshot [OPTIONS] <PATHS>...

Arguments:
  <PATHS>...  List of paths to commit

Options:
      --description <DESCRIPTION>  Snapshot description
      --full-scan                  Force a complete analysis of all files and directories
      --parent <PARENT>            Use a snapshot as parent. This snapshot will be the base when analyzing differences
      --dry-run                    Dry run
  -h, --help                       Print help
```

### Specifying a repo path and initializing a `repository`

{{backup}} stores all the data in a `Repository`. This `repository` can be store on the same machine that you used to run the tool, an external harddrive, or a machine accessible via SFTP.

If you want to initialize a repository in the local file system (your machine or a device physically connected to it), you do it like this:

```
backup --repo path/to/repo init
```

If you want to initialize a repository in a remote machine accessible via SSH which supports the SFTP protocol, you use a standardized url string like this:

```
backup --repo sftp://user@host/path/to/repo init
```

The path comes right after `user@host/`, so `sftp://user@host/home` and `sftp://user@host//home` are `home` and `/home` respectively. If no port is specified, it defaults to port 22. To specify a port, do it in a standard way like:

```
backup --repo sftp://user@host:2222/path/to/repo init
```

### Commiting a `snapshot`

### Restoring a `snapshot`

### Logging your `snapshots`
