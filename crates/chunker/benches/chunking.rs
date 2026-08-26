use std::hint::black_box;
use std::io::Cursor;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mapache_chunker::{Chunker, Normalization};

// ---------------------------------------------------------------------------
// Deterministic data generation
// ---------------------------------------------------------------------------

struct SplitMix64(u64);
impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

fn gen_random(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = SplitMix64(seed);
    let mut out = Vec::with_capacity(len);
    while out.len() + 8 <= len {
        out.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    while out.len() < len {
        out.push(rng.next_u64() as u8);
    }
    out
}

fn gen_text(len: usize, seed: u64) -> Vec<u8> {
    const WORDS: &[&str] = &[
        "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "lorem", "ipsum", "dolor",
        "sit", "amet", "fn", "let", "mut", "return", "struct", "impl", "self", "match", "async",
        "await", "value", "offset", "length", "data", "byte", "hash", "chunk", "file", "read",
        "write", "open", "close", "seek", "tell", "mode", "size", "name", "path", "dir",
    ];
    let mut rng = SplitMix64(seed);
    let mut out = Vec::with_capacity(len + 16);
    let mut col = 0;
    while out.len() < len {
        let w = WORDS[(rng.next_u64() as usize) % WORDS.len()];
        out.extend_from_slice(w.as_bytes());
        col += w.len();
        if col > 64 {
            out.push(b'\n');
            col = 0;
        } else {
            out.push(b' ');
        }
    }
    out.truncate(len);
    out
}

fn gen_zeros(len: usize) -> Vec<u8> {
    vec![0u8; len]
}

fn gen_mixed(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut s = seed;
    let mut toggle = false;
    while out.len() < len {
        let take = (64 * 1024).min(len - out.len());
        let block = if toggle {
            gen_text(take, s)
        } else {
            gen_random(take, s)
        };
        out.extend_from_slice(&block);
        toggle = !toggle;
        s = s.wrapping_add(0x1234_5678_9ABC_DEF0);
    }
    out
}

/// Repeated short pattern — compressible, frequent cut points.
fn gen_repeating(len: usize) -> Vec<u8> {
    const PATTERN: &[u8] = b"mapache-backup-v1-header-magic-bytes";
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let take = PATTERN.len().min(len - out.len());
        out.extend_from_slice(&PATTERN[..take]);
    }
    out
}

// ---------------------------------------------------------------------------
// Benchmark drivers
// ---------------------------------------------------------------------------

const MIB: usize = 1024 * 1024;

/// Drive cut() in a loop over the full buffer. Returns total chunks for
/// validation.
fn run_cut(data: &[u8], chunker: &Chunker) -> (usize, usize) {
    let mut pos = 0;
    let mut chunks = 0;
    while pos < data.len() {
        let (hash, cut) = chunker.cut(&data[pos..]);
        black_box(hash);
        pos += cut;
        chunks += 1;
    }
    (pos, chunks)
}

/// Drive ChunkStream. Returns (total_bytes, chunk_count).
fn run_stream(data: &[u8], chunker: &Chunker) -> (usize, usize) {
    let reader = Cursor::new(data);
    let stream = chunker.stream(reader);
    let mut total = 0;
    let mut chunks = 0;
    for chunk in stream {
        let c = chunk.expect("chunk error");
        total += c.length;
        chunks += 1;
    }
    (total, chunks)
}

// ---------------------------------------------------------------------------
// Benchmark groups
// ---------------------------------------------------------------------------

const DATA_64M: usize = 64 * MIB;

fn bench_cut_content(c: &mut Criterion) {
    let chunker = Chunker::new(512 * 1024, MIB, 8 * MIB, Normalization::L2);

    let inputs: [(&str, Vec<u8>); 5] = [
        ("random", gen_random(DATA_64M, 1)),
        ("text", gen_text(DATA_64M, 2)),
        ("zeros", gen_zeros(DATA_64M)),
        ("mixed", gen_mixed(DATA_64M, 3)),
        ("repeating", gen_repeating(DATA_64M)),
    ];

    let mut g = c.benchmark_group("cut_content");
    g.throughput(Throughput::Bytes(DATA_64M as u64));
    g.sample_size(50);
    g.measurement_time(std::time::Duration::from_secs(10));
    for (name, data) in &inputs {
        g.bench_function(*name, |b| {
            b.iter(|| {
                let (bytes, chunks) = run_cut(data, &chunker);
                black_box((bytes, chunks))
            })
        });
    }
    g.finish();
}

fn bench_stream_content(c: &mut Criterion) {
    let chunker = Chunker::new(512 * 1024, MIB, 8 * MIB, Normalization::L2);

    let inputs: [(&str, Vec<u8>); 5] = [
        ("random", gen_random(DATA_64M, 1)),
        ("text", gen_text(DATA_64M, 2)),
        ("zeros", gen_zeros(DATA_64M)),
        ("mixed", gen_mixed(DATA_64M, 3)),
        ("repeating", gen_repeating(DATA_64M)),
    ];

    let mut g = c.benchmark_group("stream_content");
    g.throughput(Throughput::Bytes(DATA_64M as u64));
    g.sample_size(50);
    g.measurement_time(std::time::Duration::from_secs(10));
    for (name, data) in &inputs {
        g.bench_function(*name, |b| {
            b.iter(|| {
                let (bytes, chunks) = run_stream(data, &chunker);
                black_box((bytes, chunks))
            })
        });
    }
    g.finish();
}

fn bench_cut_sizes(c: &mut Criterion) {
    let data = gen_random(64 * MIB, 7);

    let configs: [(&str, usize, usize, usize, Normalization); 4] = [
        ("4k", 2 * 1024, 4 * 1024, 16 * 1024, Normalization::L2),
        ("64k", 32 * 1024, 64 * 1024, 256 * 1024, Normalization::L2),
        ("1m", MIB / 4, MIB, 4 * MIB, Normalization::L2),
        ("4m", MIB, 4 * MIB, 16 * MIB, Normalization::L2),
    ];

    let mut g = c.benchmark_group("cut_sizes");
    g.throughput(Throughput::Bytes(data.len() as u64));
    g.sample_size(50);
    g.measurement_time(std::time::Duration::from_secs(10));
    for (name, min, normal, max, norm) in &configs {
        let chunker = Chunker::new(*min, *normal, *max, *norm);
        g.bench_function(*name, |b| {
            b.iter(|| {
                let (bytes, chunks) = run_cut(&data, &chunker);
                black_box((bytes, chunks))
            })
        });
    }
    g.finish();
}

fn bench_cut_normalization(c: &mut Criterion) {
    let data = gen_random(64 * MIB, 9);

    let mut g = c.benchmark_group("cut_normalization");
    g.throughput(Throughput::Bytes(data.len() as u64));
    g.sample_size(50);
    g.measurement_time(std::time::Duration::from_secs(10));

    let levels = [
        ("none", Normalization::None),
        ("l1", Normalization::L1),
        ("l2", Normalization::L2),
        ("l3", Normalization::L3),
    ];

    for (name, norm) in &levels {
        let chunker = Chunker::new(512 * 1024, MIB, 8 * MIB, *norm);
        g.bench_function(*name, |b| {
            b.iter(|| {
                let (bytes, chunks) = run_cut(&data, &chunker);
                black_box((bytes, chunks))
            })
        });
    }
    g.finish();
}

/// Isolate the scan loop: call cut() on progressively larger windows to
/// measure how scan cost scales with window size (min..max).
fn bench_cut_scan_length(c: &mut Criterion) {
    let chunker = Chunker::new(64, 256, 64 * 1024, Normalization::None);

    let mut g = c.benchmark_group("cut_scan_length");
    g.sample_size(200);
    g.measurement_time(std::time::Duration::from_secs(5));

    // Generate one large buffer of random data — each cut() call operates on
    // a fresh slice starting at a different offset, so we always scan from a
    // unique position.
    let data = gen_random(256 * 1024, 42);

    for &window in &[256usize, 1024, 4096, 16_384, 65_536] {
        let label = format!("w{window}");
        g.bench_function(&label, |b| {
            let mut offset = 0usize;
            b.iter(|| {
                let end = (offset + window).min(data.len());
                let (_, cut) = chunker.cut(&data[offset..end]);
                offset = (offset + cut) % (data.len() - window);
                black_box(cut)
            })
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_cut_content,
    bench_stream_content,
    bench_cut_sizes,
    bench_cut_normalization,
    bench_cut_scan_length,
);
criterion_main!(benches);
