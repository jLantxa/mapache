#![allow(dead_code, non_upper_case_globals)]

use std::time::Duration;

use criterion::Criterion;

use mapache::utils::size;

pub(crate) const KiB: usize = size::KiB as usize;
pub(crate) const MiB: usize = size::MiB as usize;
pub(crate) const GiB: usize = size::GiB as usize;

pub(crate) const TEST_KEY: [u8; 32] = *b"0123456789abcdef0123456789abcdef";

const SAMPLE_SIZE: usize = 50;
const MEASUREMENT_TIME: Duration = Duration::from_secs(10);

pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

pub(crate) fn gen_random(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = SplitMix64::new(seed);
    let mut out = Vec::with_capacity(len);
    while out.len() + 8 <= len {
        out.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    while out.len() < len {
        out.push(rng.next_u64() as u8);
    }
    out
}

pub(crate) fn gen_text(len: usize, seed: u64) -> Vec<u8> {
    const WORDS: &[&str] = &[
        "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "lorem", "ipsum", "dolor",
        "sit", "amet", "fn", "let", "mut", "return", "struct", "impl", "self", "match", "async",
        "await", "value", "offset", "length", "data", "byte", "hash", "chunk", "file", "read",
        "write", "open", "close", "seek", "tell", "mode", "size", "name", "path", "dir",
    ];
    let mut rng = SplitMix64::new(seed);
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

pub(crate) fn gen_zeros(len: usize) -> Vec<u8> {
    vec![0u8; len]
}

pub(crate) fn gen_mixed(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut s = seed;
    let mut toggle = false;
    while out.len() < len {
        let take = (64 * KiB).min(len - out.len());
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

pub(crate) fn gen_repeating(len: usize) -> Vec<u8> {
    const PATTERN: &[u8] = b"mapache-backup-v1-header-magic-bytes";
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let take = PATTERN.len().min(len - out.len());
        out.extend_from_slice(&PATTERN[..take]);
    }
    out
}

pub(crate) fn gen_ids(count: usize, seed: u64) -> Vec<mapache::common::ID> {
    use mapache::common::ID;
    let mut ids = Vec::with_capacity(count);
    let mut rng = SplitMix64::new(seed);
    for _ in 0..count {
        let mut bytes = [0u8; 32];
        for chunk in bytes.chunks_mut(8) {
            let v = rng.next_u64();
            chunk.copy_from_slice(&v.to_le_bytes());
        }
        ids.push(ID::from_bytes(bytes));
    }
    ids
}

pub(crate) fn bench_group<'a>(
    c: &'a mut Criterion,
    name: &'a str,
) -> criterion::BenchmarkGroup<'a, criterion::measurement::WallTime> {
    let mut g = c.benchmark_group(name);
    g.sample_size(SAMPLE_SIZE);
    g.measurement_time(MEASUREMENT_TIME);
    g
}
