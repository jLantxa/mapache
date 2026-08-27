mod common;

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use mapache::utils::collections::BloomFilter;

fn bench_bloom_insert(c: &mut Criterion) {
    let capacities: [(&str, usize); 4] = [
        ("1K", 1_000),
        ("10K", 10_000),
        ("100K", 100_000),
        ("1M", 1_000_000),
    ];

    let mut g = common::bench_group(c, "bloom_insert");
    for (name, cap) in &capacities {
        let ids = common::gen_ids(*cap, 42);
        g.bench_function(*name, |b| {
            b.iter_batched(
                || BloomFilter::new(*cap, 0.01),
                |mut bf| {
                    for id in &ids {
                        bf.insert(black_box(id));
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

fn bench_bloom_contains(c: &mut Criterion) {
    let capacities: [(&str, usize); 4] = [
        ("1K", 1_000),
        ("10K", 10_000),
        ("100K", 100_000),
        ("1M", 1_000_000),
    ];

    let mut g = common::bench_group(c, "bloom_contains");
    for (name, cap) in &capacities {
        let ids = common::gen_ids(*cap, 42);
        let mut bf = BloomFilter::new(*cap, 0.01);
        for id in &ids {
            bf.insert(id);
        }
        g.bench_function(*name, |b| {
            b.iter(|| {
                for id in &ids {
                    bf.contains(black_box(id));
                }
            })
        });
    }
    g.finish();
}

fn bench_bloom_contains_miss(c: &mut Criterion) {
    let capacities: [(&str, usize); 4] = [
        ("1K", 1_000),
        ("10K", 10_000),
        ("100K", 100_000),
        ("1M", 1_000_000),
    ];

    let mut g = common::bench_group(c, "bloom_contains_miss");
    for (name, cap) in &capacities {
        let ids = common::gen_ids(*cap, 42);
        let miss_ids = common::gen_ids(*cap, 99);
        let mut bf = BloomFilter::new(*cap, 0.01);
        for id in &ids {
            bf.insert(id);
        }
        g.bench_function(*name, |b| {
            b.iter(|| {
                for id in &miss_ids {
                    bf.contains(black_box(id));
                }
            })
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_bloom_insert,
    bench_bloom_contains,
    bench_bloom_contains_miss,
);
criterion_main!(benches);
