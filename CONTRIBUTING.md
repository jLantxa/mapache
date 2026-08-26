# Contributing to mapache

Thank you for considering contributing to mapache. All contributions are
welcome — bug reports, feature suggestions, documentation improvements, and
code changes.

If you're new to the project, the [user manual](doc/manual.md) and
[design document](doc/design_v2.md) are good places to understand how mapache
works.

---

## Reporting Bugs / Requesting Features

Open an [issue](https://github.com/jLantxa/mapache/issues) and describe the
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

```bash
# All tests
cargo test

# Without FUSE mount tests (macOS CI, or systems without FUSE)
cargo test -- --skip integration_tests::test_cmd_mount
```

### Building with Docker

A multi-stage Dockerfile is provided for contributors. To build with your
local changes:

```bash
sudo docker build --build-arg BUILD_SOURCE=local -t mapache-builder .
```

This builds mapache for all supported targets (Linux x64/ARM/ARMv7, Windows,
macOS) and runs tests inside the container. Useful to verify your changes
compile on all platforms without setting up cross-compilation toolchains.

To build and also extract the built binaries:

```bash
sudo python3 build_with_docker.py --local
```

### Feature Flags

- `mount` (default on Linux) — FUSE mount support. Requires `libfuse-dev` to
  build.


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
