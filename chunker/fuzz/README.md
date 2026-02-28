# Fuzzy Testing for Mapache Chunker

This directory contains fuzzing targets for the `chunker` crate using `cargo-fuzz`.

## Setup

You need to install `cargo-fuzz` and have the **nightly** toolchain installed:

```bash
cargo install cargo-fuzz
rustup toolchain install nightly
```

## Running the Fuzzer

To start fuzzing the `ChunkStream` implementation, you must use the nightly toolchain:

```bash
cargo +nightly fuzz run chunker_stream
```

The fuzzer will:
1. Randomly generate `min_size`, `normal_size`, `max_size`, and `Normalization` level.
2. Feed random data into the `ChunkStream`.
3. Verify that the chunks can be perfectly reconstructed into the original data.
4. Verify that all chunks (except the last one) respect the `min_size` and `max_size` constraints.
5. Ensure the offsets and lengths reported by the chunks are consistent.
