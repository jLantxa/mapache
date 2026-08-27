mod common;

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use mapache::repository::storage::SecureStorage;

fn bench_compress(c: &mut Criterion) {
    let ss = SecureStorage::new();
    let data_sizes: [(&str, usize); 5] = [
        ("64KiB", 64 * common::KiB),
        ("1MiB", common::MiB),
        ("4MiB", 4 * common::MiB),
        ("16MiB", 16 * common::MiB),
        ("64MiB", 64 * common::MiB),
    ];

    let mut g = common::bench_group(c, "compress");
    for (name, size) in &data_sizes {
        let data = common::gen_random(*size, 42);
        g.throughput(Throughput::Bytes(*size as u64));
        g.bench_function(*name, |b| {
            b.iter(|| black_box(ss.compress(black_box(&data)).expect("compress")))
        });
    }
    g.finish();
}

fn bench_decompress(c: &mut Criterion) {
    let ss = SecureStorage::new();
    let data_sizes: [(&str, usize); 5] = [
        ("64KiB", 64 * common::KiB),
        ("1MiB", common::MiB),
        ("4MiB", 4 * common::MiB),
        ("16MiB", 16 * common::MiB),
        ("64MiB", 64 * common::MiB),
    ];

    let mut g = common::bench_group(c, "decompress");
    for (name, size) in &data_sizes {
        let data = common::gen_random(*size, 42);
        let compressed = ss.compress(&data).expect("compress");
        g.throughput(Throughput::Bytes(*size as u64));
        g.bench_function(*name, |b| {
            b.iter(|| black_box(ss.decompress(black_box(&compressed)).expect("decompress")))
        });
    }
    g.finish();
}

fn bench_compress_content(c: &mut Criterion) {
    let ss = SecureStorage::new();
    let inputs: [(&str, Vec<u8>); 5] = [
        ("random", common::gen_random(common::MiB, 10)),
        ("text", common::gen_text(common::MiB, 11)),
        ("zeros", common::gen_zeros(common::MiB)),
        ("mixed", common::gen_mixed(common::MiB, 12)),
        ("repeating", common::gen_repeating(common::MiB)),
    ];

    let mut g = common::bench_group(c, "compress_content");
    g.throughput(Throughput::Bytes(common::MiB as u64));
    for (name, data) in &inputs {
        g.bench_function(*name, |b| {
            b.iter(|| black_box(ss.compress(data).expect("compress")))
        });
    }
    g.finish();
}

fn bench_compress_levels(c: &mut Criterion) {
    let data = common::gen_text(common::MiB, 30);
    let levels: [(&str, i32); 5] = [
        ("fastest", 1),
        ("fast", 3),
        ("balanced", 5),
        ("better", 10),
        ("best", 19),
    ];

    let mut g = common::bench_group(c, "compress_levels");
    g.throughput(Throughput::Bytes(common::MiB as u64));
    for (name, level) in &levels {
        let ss = SecureStorage::new().with_compression(*level);
        g.bench_function(*name, |b| {
            b.iter(|| black_box(ss.compress(&data).expect("compress")))
        });
    }
    g.finish();
}

fn bench_encrypt(c: &mut Criterion) {
    let ss = SecureStorage::new()
        .with_key(&common::TEST_KEY)
        .expect("valid key");

    let data_sizes: [(&str, usize); 5] = [
        ("64KiB", 64 * common::KiB),
        ("1MiB", common::MiB),
        ("4MiB", 4 * common::MiB),
        ("16MiB", 16 * common::MiB),
        ("64MiB", 64 * common::MiB),
    ];

    let mut g = common::bench_group(c, "encrypt");
    for (name, size) in &data_sizes {
        let data = common::gen_random(*size, 42);
        g.throughput(Throughput::Bytes(*size as u64));
        g.bench_function(*name, |b| {
            b.iter(|| black_box(ss.encrypt(black_box(&data)).expect("encrypt")))
        });
    }
    g.finish();
}

fn bench_decrypt(c: &mut Criterion) {
    let ss = SecureStorage::new()
        .with_key(&common::TEST_KEY)
        .expect("valid key");

    let data_sizes: [(&str, usize); 5] = [
        ("64KiB", 64 * common::KiB),
        ("1MiB", common::MiB),
        ("4MiB", 4 * common::MiB),
        ("16MiB", 16 * common::MiB),
        ("64MiB", 64 * common::MiB),
    ];

    let mut g = common::bench_group(c, "decrypt");
    for (name, size) in &data_sizes {
        let data = common::gen_random(*size, 42);
        let encrypted = ss.encrypt(&data).expect("encrypt");
        g.throughput(Throughput::Bytes(*size as u64));
        g.bench_function(*name, |b| {
            b.iter(|| black_box(ss.decrypt(black_box(&encrypted)).expect("decrypt")))
        });
    }
    g.finish();
}

fn bench_encode_decode_sizes(c: &mut Criterion) {
    let ss = SecureStorage::new()
        .with_compression(3)
        .with_key(&common::TEST_KEY)
        .expect("valid key");

    let data_sizes: [(&str, usize); 5] = [
        ("64KiB", 64 * common::KiB),
        ("1MiB", common::MiB),
        ("4MiB", 4 * common::MiB),
        ("16MiB", 16 * common::MiB),
        ("64MiB", 64 * common::MiB),
    ];

    let mut g = common::bench_group(c, "encode_decode_sizes");
    for (name, size) in &data_sizes {
        let data = common::gen_random(*size, 42);
        let encoded = ss.encode(&data).expect("encode");
        g.throughput(Throughput::Bytes(*size as u64));
        g.bench_function(format!("{name}_encode"), |b| {
            b.iter(|| black_box(ss.encode(black_box(&data)).expect("encode")))
        });
        g.bench_function(format!("{name}_decode"), |b| {
            b.iter(|| black_box(ss.decode(black_box(&encoded)).expect("decode")))
        });
    }
    g.finish();
}

fn bench_encode_decode_content(c: &mut Criterion) {
    let ss = SecureStorage::new()
        .with_compression(3)
        .with_key(&common::TEST_KEY)
        .expect("valid key");

    let inputs: [(&str, Vec<u8>); 5] = [
        ("random", common::gen_random(common::MiB, 10)),
        ("text", common::gen_text(common::MiB, 11)),
        ("zeros", common::gen_zeros(common::MiB)),
        ("mixed", common::gen_mixed(common::MiB, 12)),
        ("repeating", common::gen_repeating(common::MiB)),
    ];

    let mut g = common::bench_group(c, "encode_decode_content");
    g.throughput(Throughput::Bytes(common::MiB as u64));
    for (name, data) in &inputs {
        let encoded = ss.encode(data).expect("encode");
        g.bench_function(format!("{name}_encode"), |b| {
            b.iter(|| black_box(ss.encode(black_box(data)).expect("encode")))
        });
        g.bench_function(format!("{name}_decode"), |b| {
            b.iter(|| black_box(ss.decode(black_box(&encoded)).expect("decode")))
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_compress,
    bench_decompress,
    bench_compress_content,
    bench_compress_levels,
    bench_encrypt,
    bench_decrypt,
    bench_encode_decode_sizes,
    bench_encode_decode_content,
);
criterion_main!(benches);
