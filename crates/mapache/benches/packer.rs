mod common;

use std::{hint::black_box, sync::Arc};

use criterion::{Criterion, criterion_group, criterion_main};

use mapache::{
    common::{BlobType, ID},
    repository::{packer::Packer, storage::SecureStorage},
};

fn bench_packer_add_blob_sizes(c: &mut Criterion) {
    let ss = Arc::new(
        SecureStorage::new()
            .with_key(&common::TEST_KEY)
            .expect("valid key"),
    );

    let blob_sizes: [(&str, usize); 5] = [
        ("1KB", 1024),
        ("64KB", 64 * common::KiB),
        ("256KB", 256 * common::KiB),
        ("1MiB", common::MiB),
        ("4MiB", 4 * common::MiB),
    ];

    let mut g = common::bench_group(c, "packer_add_blob_sizes");
    for (name, size) in &blob_sizes {
        let data = common::gen_random(*size, 42);
        let encoded = ss.encode(&data).expect("encode");
        let id = ID::from_content(&data);
        g.bench_function(*name, |b| {
            b.iter_batched(
                || Packer::new(*size * 2, ss.clone()).expect("packer new"),
                |mut packer| {
                    packer
                        .add_blob(
                            black_box(id),
                            BlobType::Data,
                            black_box(&encoded),
                            *size as u64,
                            false,
                        )
                        .expect("add_blob");
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

fn bench_packer_add_blob_throughput(c: &mut Criterion) {
    let ss = Arc::new(
        SecureStorage::new()
            .with_key(&common::TEST_KEY)
            .expect("valid key"),
    );

    let data = common::gen_random(256 * common::KiB, 42);
    let encoded = ss.encode(&data).expect("encode");

    let max_count = (mapache::common::defaults::MAX_PACK_SIZE as usize) / encoded.len();
    let counts: [(&str, usize); 4] = [
        ("100", 100),
        ("1K", 1_000),
        ("10K", 10_000.min(max_count)),
        ("100K", 100_000.min(max_count)),
    ];

    let mut g = common::bench_group(c, "packer_add_blob_throughput");
    for (name, count) in &counts {
        let buf_size = *count * encoded.len();
        g.bench_function(*name, |b| {
            b.iter_batched(
                || Packer::new(buf_size, ss.clone()).expect("packer new"),
                |mut packer| {
                    for i in 0..*count {
                        let id = ID::from_content(i.to_le_bytes());
                        packer
                            .add_blob(
                                black_box(id),
                                BlobType::Data,
                                black_box(&encoded),
                                data.len() as u64,
                                false,
                            )
                            .expect("add_blob");
                    }
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_packer_add_blob_sizes,
    bench_packer_add_blob_throughput,
);
criterion_main!(benches);
