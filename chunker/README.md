# Mapache Chunker

A standalone [FastCDC](https://ieeexplore.ieee.org/document/9055082) (Fast
Content-Defined Chunking) implementation in Rust.

## About

Content-Defined Chunking (CDC) splits data into variable-sized chunks based on
the content itself, rather than fixed byte offsets. This makes it ideal for data
deduplication: when files are modified, chunk boundaries tend to remain stable,
so only the changed chunks need to be stored.

This crate implements **FastCDC** by Wen Xia et al. (2020), which achieves
3–12× speedup over Rabin-based CDC with comparable deduplication ratios. All
five optimizations described in the paper are included:

- **Gear-based rolling hash** — fast hash using precomputed lookup tables
- **Optimized hash judgment** — fast mask-based boundary detection
- **Sub-minimum chunk cut-point skipping** — skip hash checks before `min_size`
- **Normalized chunking** — four levels (`None`, `L1`, `L2`, `L3`) controlling
  chunk size distribution width
- **Rolling two bytes each time** — processes two bytes per loop iteration

## Usage

```rust
use chunker::{Chunker, Normalization};

let chunker = Chunker::new(
    512 * 1024,       // min_size:      512 KiB
    1024 * 1024,      // normal_size:   1 MiB
    8 * 1024 * 1024,  // max_size:      8 MiB
    Normalization::L2,
);

let data: &[u8] = b"some content to chunk";
let cursor = std::io::Cursor::new(data);

for result in chunker.stream(cursor) {
    let chunk = result.unwrap();
    println!(
        "offset={} length={} size={}",
        chunk.offset,
        chunk.length,
        chunk.data.len(),
    );
}
```

## Lookup Tables

The Gear hash tables and masks are precomputed in `src/lookup.rs`. To regenerate
them:

```bash
cargo run --bin chunker-lookup
```

## Fuzzing

See [`fuzz/README.md`](fuzz/README.md) for instructions on running the fuzz
targets.

## Paper

Wen Xia, et al. "FastCDC: a Fast and Efficient Content-Defined Chunking
Approach for Data Deduplication." *IEEE/ACM Transactions on Networking*,
vol. 28, no. 4, 2020, pp. 1824–1837.
[DOI: 10.1109/TNET.2020.2992305](https://ieeexplore.ieee.org/document/9055082)

## License

GPL-3.0-only
