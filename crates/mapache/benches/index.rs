mod common;

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use mapache::{
    common::{BlobType, ID},
    repository::{
        index::{
            Index, IndexFile, IndexFileBlob, IndexFilePack, deserialize_index_binary,
            serialize_index_binary,
        },
        packer::PackedBlobDescriptor,
    },
};

fn make_descriptors(ids: &[ID], blob_type: BlobType) -> Vec<PackedBlobDescriptor> {
    ids.iter()
        .enumerate()
        .map(|(i, id)| PackedBlobDescriptor {
            id: *id,
            blob_type,
            offset: (i as u32) * 1024,
            length: 1024,
            raw_length: 1024,
            compressed: true,
        })
        .collect()
}

fn make_index_file(pack_id: ID, ids: &[ID]) -> IndexFile {
    IndexFile {
        packs: vec![IndexFilePack {
            id: pack_id,
            blobs: ids
                .iter()
                .map(|id| IndexFileBlob {
                    id: *id,
                    blob_type: BlobType::Data,
                    offset: 0,
                    length: 1024,
                    raw_length: 1024,
                    compressed: true,
                })
                .collect(),
        }],
    }
}

const CAPACITIES: [(&str, usize); 4] = [
    ("1K", 1_000),
    ("10K", 10_000),
    ("100K", 100_000),
    ("1M", 1_000_000),
];

fn bench_index_build(c: &mut Criterion) {
    let mut g = common::bench_group(c, "index_build");
    for (name, count) in &CAPACITIES {
        let ids = common::gen_ids(*count, 42);
        let pack_id = ID::from_content(b"pack0");
        let descriptors = make_descriptors(&ids, BlobType::Data);
        g.bench_function(*name, |b| {
            b.iter_batched(
                || (Index::new(), descriptors.clone()),
                |(mut idx, desc)| {
                    idx.add_pack(&pack_id, desc);
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

fn bench_index_lookup(c: &mut Criterion) {
    let mut g = common::bench_group(c, "index_lookup");
    for (name, count) in &CAPACITIES {
        let ids = common::gen_ids(*count, 42);
        let pack_id = ID::from_content(b"pack0");
        let mut idx = Index::new();
        idx.add_pack(&pack_id, make_descriptors(&ids, BlobType::Data));
        g.bench_function(*name, |b| {
            b.iter(|| {
                for id in &ids {
                    idx.get(black_box(id));
                }
            })
        });
    }
    g.finish();
}

fn bench_index_freeze(c: &mut Criterion) {
    let mut g = common::bench_group(c, "index_freeze");
    for (name, count) in &CAPACITIES {
        let ids = common::gen_ids(*count, 42);
        let pack_id = ID::from_content(b"pack0");
        let descriptors = make_descriptors(&ids, BlobType::Data);
        g.bench_function(*name, |b| {
            b.iter_batched(
                || {
                    let mut idx = Index::new();
                    idx.add_pack(&pack_id, descriptors.clone());
                    idx
                },
                |mut idx| {
                    idx.finalize();
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

fn bench_index_serialize(c: &mut Criterion) {
    let mut g = common::bench_group(c, "index_serialize");
    for (name, count) in &CAPACITIES {
        let ids = common::gen_ids(*count, 42);
        let index_file = make_index_file(ID::from_content(b"pack0"), &ids);
        g.bench_function(*name, |b| {
            b.iter(|| black_box(serialize_index_binary(black_box(&index_file))))
        });
    }
    g.finish();
}

fn bench_index_deserialize(c: &mut Criterion) {
    let mut g = common::bench_group(c, "index_deserialize");
    for (name, count) in &CAPACITIES {
        let ids = common::gen_ids(*count, 42);
        let serialized = serialize_index_binary(&make_index_file(ID::from_content(b"pack0"), &ids));
        g.bench_function(*name, |b| {
            b.iter(|| {
                black_box(deserialize_index_binary(black_box(&serialized)).expect("deserialize"))
            })
        });
    }
    g.finish();
}

fn bench_index_from_index_file(c: &mut Criterion) {
    let mut g = common::bench_group(c, "index_from_index_file");
    for (name, count) in &CAPACITIES {
        let ids = common::gen_ids(*count, 42);
        let pack_id = ID::from_content(b"pack0");
        let index_file = make_index_file(pack_id, &ids);
        g.bench_function(*name, |b| {
            b.iter(|| {
                black_box(Index::from_index_file(
                    black_box(index_file.clone()),
                    pack_id,
                ))
            })
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_index_build,
    bench_index_lookup,
    bench_index_freeze,
    bench_index_serialize,
    bench_index_deserialize,
    bench_index_from_index_file,
);
criterion_main!(benches);
