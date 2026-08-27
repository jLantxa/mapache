mod common;

use std::{collections::HashMap, hint::black_box, io::Cursor, sync::Mutex};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use mapache::{
    backend::WriteContents,
    common::{BlobType, ID, SaveID, traits::BlobSaver},
    repository::storage::SecureStorage,
};

struct HashingBlobSaver;

impl BlobSaver for HashingBlobSaver {
    fn save_blob(
        &self,
        _blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
    ) -> mapache::common::error::Result<ID> {
        Ok(match save_id {
            SaveID::CalculateID => ID::from_content(data.as_ref()),
            SaveID::WithID(id) => id,
        })
    }
}

struct EncodingBlobSaver {
    storage: SecureStorage,
    blobs: Mutex<HashMap<ID, Vec<u8>>>,
}

impl EncodingBlobSaver {
    fn new(compression_level: i32) -> Self {
        Self {
            storage: SecureStorage::new()
                .with_compression(compression_level)
                .with_key(&common::TEST_KEY)
                .expect("valid key"),
            blobs: Mutex::new(HashMap::new()),
        }
    }
}

impl BlobSaver for EncodingBlobSaver {
    fn save_blob(
        &self,
        _blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
    ) -> mapache::common::error::Result<ID> {
        let id = match save_id {
            SaveID::CalculateID => ID::from_content(data.as_ref()),
            SaveID::WithID(id) => id,
        };
        let encoded = self.storage.encode(data.as_ref())?;
        self.blobs
            .lock()
            .expect("mutex poisoned")
            .insert(id, encoded);
        Ok(id)
    }
}

const CHUNK_BENCH_SIZE: usize = 16 * common::MiB;

fn bench_chunker_hash_only(c: &mut Criterion) {
    let chunker = mapache_chunker::Chunker::new(
        512 * common::KiB,
        common::MiB,
        8 * common::MiB,
        mapache_chunker::Normalization::L2,
    );

    let inputs: [(&str, Vec<u8>); 3] = [
        ("text", common::gen_text(CHUNK_BENCH_SIZE, 10)),
        ("random", common::gen_random(CHUNK_BENCH_SIZE, 11)),
        ("mixed", common::gen_mixed(CHUNK_BENCH_SIZE, 12)),
    ];

    let saver = HashingBlobSaver;
    let mut g = common::bench_group(c, "archiver_chunk_hash");
    g.throughput(Throughput::Bytes(CHUNK_BENCH_SIZE as u64));
    for (name, data) in &inputs {
        g.bench_function(*name, |b| {
            b.iter(|| {
                let stream =
                    mapache_chunker::ChunkStream::new(Cursor::new(&data), &chunker, data.len());
                let ids: Vec<_> = stream
                    .map(|c| {
                        let c = c.expect("chunk");
                        saver
                            .save_blob(
                                BlobType::Data,
                                WriteContents::Borrowed(&c.data),
                                SaveID::CalculateID,
                            )
                            .expect("save")
                    })
                    .collect();
                black_box(ids)
            })
        });
    }
    g.finish();
}

fn bench_chunker_hash_encode(c: &mut Criterion) {
    let chunker = mapache_chunker::Chunker::new(
        512 * common::KiB,
        common::MiB,
        8 * common::MiB,
        mapache_chunker::Normalization::L2,
    );

    let inputs: [(&str, Vec<u8>); 3] = [
        ("text", common::gen_text(CHUNK_BENCH_SIZE, 20)),
        ("random", common::gen_random(CHUNK_BENCH_SIZE, 21)),
        ("mixed", common::gen_mixed(CHUNK_BENCH_SIZE, 22)),
    ];

    let saver = EncodingBlobSaver::new(3);
    let mut g = common::bench_group(c, "archiver_chunk_hash_encode");
    g.throughput(Throughput::Bytes(CHUNK_BENCH_SIZE as u64));
    for (name, data) in &inputs {
        g.bench_function(*name, |b| {
            b.iter(|| {
                let stream =
                    mapache_chunker::ChunkStream::new(Cursor::new(&data), &chunker, data.len());
                let ids: Vec<_> = stream
                    .map(|c| {
                        let c = c.expect("chunk");
                        saver
                            .save_blob(
                                BlobType::Data,
                                WriteContents::Borrowed(&c.data),
                                SaveID::CalculateID,
                            )
                            .expect("save")
                    })
                    .collect();
                black_box(ids)
            })
        });
    }
    g.finish();
}

fn bench_small_file_pipeline(c: &mut Criterion) {
    let sizes: [(&str, usize); 4] = [
        ("1KiB", 1024),
        ("4KiB", 4096),
        ("64KiB", 64 * common::KiB),
        ("512KiB", 512 * common::KiB),
    ];

    let saver = EncodingBlobSaver::new(3);
    let mut g = common::bench_group(c, "archiver_small_file");
    for (name, size) in &sizes {
        let data = common::gen_text(*size, 30);
        g.throughput(Throughput::Bytes(*size as u64));
        g.bench_function(*name, |b| {
            b.iter(|| {
                black_box(
                    saver
                        .save_blob(
                            BlobType::Data,
                            WriteContents::Borrowed(black_box(&data)),
                            SaveID::CalculateID,
                        )
                        .expect("save"),
                )
            })
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_chunker_hash_only,
    bench_chunker_hash_encode,
    bench_small_file_pipeline,
);
criterion_main!(benches);
