mod common;

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use mapache::common::ID;

fn bench_hash_sizes(c: &mut Criterion) {
    let sizes: [(&str, usize); 6] = [
        ("1KiB", 1024),
        ("64KiB", 64 * common::KiB),
        ("1MiB", common::MiB),
        ("4MiB", 4 * common::MiB),
        ("16MiB", 16 * common::MiB),
        ("64MiB", 64 * common::MiB),
    ];

    let mut g = common::bench_group(c, "hash_sizes");
    for (name, size) in &sizes {
        let data = common::gen_random(*size, 42);
        g.throughput(Throughput::Bytes(*size as u64));
        g.bench_function(*name, |b| {
            b.iter(|| black_box(ID::from_content(black_box(&data))))
        });
    }
    g.finish();
}

fn bench_hash_content(c: &mut Criterion) {
    let inputs: [(&str, Vec<u8>); 5] = [
        ("random", common::gen_random(common::MiB, 10)),
        ("text", common::gen_text(common::MiB, 11)),
        ("zeros", common::gen_zeros(common::MiB)),
        ("mixed", common::gen_mixed(common::MiB, 12)),
        ("repeating", common::gen_repeating(common::MiB)),
    ];

    let mut g = common::bench_group(c, "hash_content");
    g.throughput(Throughput::Bytes(common::MiB as u64));
    for (name, data) in &inputs {
        g.bench_function(*name, |b| {
            b.iter(|| black_box(ID::from_content(black_box(&data))))
        });
    }
    g.finish();
}

fn bench_hash_incremental(c: &mut Criterion) {
    let data = common::gen_random(64 * common::MiB, 20);

    let mut g = common::bench_group(c, "hash_incremental");
    g.throughput(Throughput::Bytes(data.len() as u64));
    g.bench_function("64MiB_1MiB_chunks", |b| {
        b.iter(|| {
            let ids: Vec<_> = data
                .chunks(common::MiB)
                .map(|chunk| ID::from_content(black_box(chunk)))
                .collect();
            black_box(ids)
        })
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_hash_sizes,
    bench_hash_content,
    bench_hash_incremental,
);
criterion_main!(benches);
