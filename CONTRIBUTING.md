# Contributing to mapache

Thank you for considering contributing to mapache. All contributions are
welcome — bug reports, feature suggestions, documentation improvements, and
code changes.

If you're new to the project, the [user manual](doc/manual.md) and
[design document](doc/design_v2.md) are good places to understand how mapache
works.

---

## Reporting Bugs / Requesting Features

Open an [issue](https://github.com/jlantxa/mapache/issues) and describe the
problem or suggestion. Helpful details to include:

- Version of mapache (`mapache --version`).
- Your operating system and environment.
- What you expected and what actually happened.

I cannot guarantee every suggestion will be implemented, but I will review
and consider each one.

---

## Pull Requests

Pull requests are welcome. If you cannot create a branch on the main repo,
fork the repository and open the PR from there.

Every PR should include:

- A clear description of what the change does and why.
- The version of mapache you tested against.
- Any related issue number.

### Code Quality

Before opening the PR, please make sure:

- `cargo fmt` is clean (CI will check).
- `cargo clippy --all-targets --all-features -- -D warnings` passes.
- All existing tests pass. If you add functionality, include tests.
  If you fix a bug, consider adding a test that reproduces it.

### Commit Messages

No strict format required. Keep messages brief, descriptive, and clean.
The project loosely follows conventional commits (`feat:`, `fix:`,
`refactor:`, `chore:`, `docs:`, `perf:`, `test:`) but clarity is what
matters most.

### AI-Assisted Contributions

AI-generated code is welcome as long as the quality justifies it. You are
responsible for every change you submit — please review and understand it
before opening the PR. I may ask you to explain or modify your approach.

### Running Tests

CI runs the following, so please run them locally before opening a PR:

```bash
# All tests (Linux/Windows, same as CI)
cargo test --locked

# macOS: build without the FUSE `mount` feature
cargo test --locked --no-default-features
```

### Building with Docker

A Docker image with all cross-compilation toolchains is provided for
contributors. Build the image once:

```bash
docker build -t mapache-builder tools/docker
```

Build your local changes for a single target (the project directory is
mounted into the container):

```bash
docker run --rm -v "$(pwd)":/mapache mapache-builder \
  sh /build-target.sh x86_64-unknown-linux-musl \
    "-C target-feature=+crt-static" "" build
```

Or build every supported target (Linux/ARM, Android, Windows, macOS) with
the helper script, which packs the results under `build/`:

```bash
python3 tools/docker/build.py --ref <label>                  # all targets
python3 tools/docker/build.py --ref <label> --target linux   # subset
```

Useful to verify your changes compile on all platforms without setting up
cross-compilation toolchains. Note that `--ref` only names the output
directory — the build always compiles your current working tree, not a git
tag.

### Feature Flags

- `mount` (default) — FUSE mount support (Unix). To build with it on macOS
  you need macFUSE installed (`brew install --cask macfuse`); Linux needs
  no headers to build — only the `fuse3` package at runtime, when using
  `mount`.

```bash
cargo build --no-default-features   # without mount
cargo build --all-features          # with everything
```

---

## License

By contributing, you agree that your contributions will be licensed under
the [GNU General Public License v3](LICENSE).

---

## Code of Conduct

Be respectful. Disagreements are fine; personal attacks are not.
