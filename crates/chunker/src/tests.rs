use std::io::Cursor;

use rstest::rstest;

use crate::{Chunker, Normalization, Result, lookup::MASKS};

#[allow(non_upper_case_globals)]
const kiB: usize = 1024;
#[allow(non_upper_case_globals)]
const MiB: usize = 1024 * kiB;

const MAPACHE_PNG: &[u8] = include_bytes!("../testdata/mapache.png");

#[rstest]
#[case(0, 0, 0)]
#[case(0, 0, 32 * 1024 * 1024)]
#[case(0, 32 * 1024 * 1024, 0)]
#[case(32 * 1024 * 1024, 0, 0)]
#[should_panic]
fn test_create_chunker(#[case] min: usize, #[case] normal: usize, #[case] max: usize) {
    let _ = Chunker::new(min, normal, max, Normalization::None);
}

#[test]
fn test_mask_distributions() {
    for (n, mask) in MASKS.iter().enumerate() {
        assert_eq!(mask.count_ones() as usize, n);
    }
}

#[test]
fn test_rounded_log2_matches_nearest_bucket() {
    use crate::rounded_log2;

    // Powers of two: exact, no rounding needed.
    assert_eq!(rounded_log2(1024), 10);
    assert_eq!(rounded_log2(16384), 14);
    // Non-powers of two: must round to nearest in log-space, not floor.
    assert_eq!(rounded_log2(1500), 11); // log2 ~ 10.55, rounds up
    assert_eq!(rounded_log2(12288), 14); // log2 ~ 13.585, rounds up
    assert_eq!(rounded_log2(24576), 15); // log2 ~ 14.585, rounds up
    assert_eq!(rounded_log2(1100), 10); // log2 ~ 10.103, rounds down
}

#[rstest]
#[case(Normalization::None)]
fn test_chunker_masks(#[case] normalization: Normalization) {
    let chunker = Chunker::new(64, 256, 1024, normalization);
    assert_eq!(chunker.mask_s, MASKS[8 + normalization.bits()]);
    assert_eq!(chunker.mask_l, MASKS[8 - normalization.bits()]);

    let chunker = Chunker::new(8 * kiB, 16 * kiB, 32 * kiB, normalization);
    assert_eq!(chunker.mask_s, MASKS[14 + normalization.bits()]);
    assert_eq!(chunker.mask_l, MASKS[14 - normalization.bits()]);

    let chunker = Chunker::new(MiB, 4 * MiB, 16 * MiB, normalization);
    assert_eq!(chunker.mask_s, MASKS[22 + normalization.bits()]);
    assert_eq!(chunker.mask_l, MASKS[22 - normalization.bits()]);
}

#[test]
fn test_chunk_empty_source() {
    let data = Vec::new();
    let chunker = Chunker::new(100, 4 * kiB, 8 * kiB, Normalization::None);

    let cursor = Cursor::new(data);
    let chunks: Vec<_> = chunker.stream(cursor).flatten().collect();
    assert_eq!(chunks.len(), 0);
}

#[test]
fn test_cut_respects_min_size() {
    let min_size = 100;
    let normal_size = 4 * kiB;
    let max_size = 8 * kiB;

    let chunker = Chunker::new(min_size, normal_size, max_size, Normalization::None);

    let data_len = max_size - 100;
    let data = vec![0u8; data_len];

    let tiny_data = &data[..min_size - 1];
    let (_hash, cut) = chunker.cut(tiny_data);
    assert_eq!(cut, min_size - 1, "Should return full length if < MIN_SIZE");

    let large_data = &data[..];
    let (_hash, cut) = chunker.cut(large_data);
    assert_eq!(
        cut, data_len,
        "Should return full length/MAX_CAP if no pattern match"
    );
}

#[test]
fn test_cut_hits_max_size_boundary() {
    let min_size = 128;
    let normal_size = kiB;
    let max_size = 4 * kiB;
    let chunker = Chunker::new(min_size, normal_size, max_size, Normalization::None);

    let data_max = vec![0u8; max_size];
    let (_hash, cut_max) = chunker.cut(&data_max);
    assert_eq!(cut_max, max_size, "Should return MAX_SIZE");

    let data_too_big = vec![0u8; max_size + 500];
    let (_hash, cut_too_big) = chunker.cut(&data_too_big);
    assert_eq!(cut_too_big, max_size, "Should cap at MAX_SIZE limit");
}

#[test]
fn test_cut_on_random_data_is_not_max_size() {
    let min_size = 128;
    let normal_size = 4 * kiB;
    let max_size = 8 * kiB;
    let chunker = Chunker::new(min_size, normal_size, max_size, Normalization::None);

    let data_len = 2 * max_size;
    let data: Vec<u8> = (0..data_len).map(|index| (index % 251) as u8).collect();

    let cut_point = chunker.cut(&data).1;

    assert!(
        cut_point < data_len,
        "Should have found a cut point before MAX_CAP"
    );
    assert!(
        cut_point >= chunker.min_size,
        "Cut point must be >= MIN_SIZE"
    );
}

#[test]
fn test_chunking_reconstructs_input_with_contiguous_offsets() -> Result<()> {
    let data: Vec<u8> = (0..(16 * kiB + 37))
        .map(|index| (index % 251) as u8)
        .collect();
    let chunker = Chunker::new(kiB, 4 * kiB, 8 * kiB, Normalization::L2);
    let chunks: Vec<_> = chunker.stream(Cursor::new(&data)).collect::<Result<_>>()?;

    let mut reconstructed = Vec::with_capacity(data.len());
    let mut expected_offset = 0;
    for (index, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk.offset, expected_offset);
        assert_eq!(chunk.length, chunk.data.len());
        if index + 1 < chunks.len() {
            assert!(chunk.length >= chunker.min_size);
            assert!(chunk.length <= chunker.max_size);
        }
        expected_offset += chunk.length;
        reconstructed.extend_from_slice(&chunk.data);
    }

    assert_eq!(expected_offset, data.len());
    assert_eq!(reconstructed, data);
    Ok(())
}

#[test]
fn test_deterministic_chunks_4k_l2() -> Result<()> {
    let data: Vec<u8> = MAPACHE_PNG.to_vec();
    let size = data.len();
    let chunker = Chunker::new(kiB, 4 * kiB, 8 * kiB, Normalization::L2);

    let expected_lens: Vec<usize> = vec![
        4390, 4154, 7076, 4405, 4660, 1450, 4980, 8192, 5097, 1044, 6644, 5231, 5991, 4368, 4232,
        4749, 5517, 6227, 4687, 1894, 5005, 6310, 3190, 4524, 6829, 5619, 4211, 5818, 6696, 2492,
        4181, 5955, 5060, 1560, 4145, 1487, 5256, 4447, 3191, 4290, 4163, 7905, 1218, 4879, 5272,
        4720, 4659, 6075, 5366, 4644, 1347, 4107, 1679, 4201,
    ];

    let cursor = Cursor::new(data);

    let actual_lens: Vec<usize> = chunker
        .stream(cursor)
        .map(|chunk| chunk.unwrap().length)
        .collect();
    let total_bytes_chunked: usize = actual_lens.iter().sum();

    assert_eq!(
        actual_lens, expected_lens,
        "Chunk lengths must be deterministic."
    );
    assert_eq!(
        total_bytes_chunked, size,
        "Total chunked bytes must match input data size."
    );

    Ok(())
}

#[test]
fn test_deterministic_chunks_32k_l2() -> Result<()> {
    let data: Vec<u8> = MAPACHE_PNG.to_vec();
    let size = data.len();
    let chunker = Chunker::new(8 * kiB, 32 * kiB, 64 * kiB, Normalization::L2);

    let expected_lens: Vec<usize> = vec![41844, 36372, 33780, 42120, 40715, 42895, 7763];

    let cursor = Cursor::new(data);

    let actual_lens: Vec<usize> = chunker
        .stream(cursor)
        .map(|chunk| chunk.unwrap().length)
        .collect();
    let total_bytes_chunked: usize = actual_lens.iter().sum();

    assert_eq!(
        actual_lens, expected_lens,
        "Chunk lengths must be deterministic."
    );
    assert_eq!(
        total_bytes_chunked, size,
        "Total chunked bytes must match input data size."
    );

    Ok(())
}

#[test]
fn test_deterministic_chunks_32k_l0() -> Result<()> {
    let data: Vec<u8> = MAPACHE_PNG.to_vec();
    let size = data.len();
    let chunker = Chunker::new(8 * kiB, 32 * kiB, 64 * kiB, Normalization::None);

    let expected_lens: Vec<usize> = vec![65536, 44602, 65536, 65536, 4279];

    let cursor = Cursor::new(data);

    let actual_lens: Vec<usize> = chunker
        .stream(cursor)
        .map(|chunk| chunk.unwrap().length)
        .collect();
    let total_bytes_chunked: usize = actual_lens.iter().sum();

    assert_eq!(
        actual_lens, expected_lens,
        "Chunk lengths must be deterministic."
    );
    assert_eq!(
        total_bytes_chunked, size,
        "Total chunked bytes must match input data size."
    );

    Ok(())
}

#[test]
fn test_chunk_very_small_data() {
    let data = vec![1, 2, 3];
    let chunker = Chunker::new(64, 256, 1024, Normalization::None);

    let cursor = Cursor::new(data.clone());
    let chunks: Vec<_> = chunker.stream(cursor).map(|c| c.unwrap()).collect();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].data, data);
    assert_eq!(chunks[0].offset, 0);
}

#[test]
fn test_chunk_exactly_min_size() {
    let min_size = 64;
    let data = vec![0u8; min_size];
    let chunker = Chunker::new(min_size, 256, 1024, Normalization::None);

    let cursor = Cursor::new(data.clone());
    let chunks: Vec<_> = chunker.stream(cursor).map(|c| c.unwrap()).collect();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].length, min_size);
}

#[test]
fn test_chunk_large_repeated_data() {
    // Repeated data often hits max_size if no anchor is found
    let max_size = 1024;
    let data = vec![0u8; max_size * 3];
    let chunker = Chunker::new(64, 256, max_size, Normalization::None);

    let cursor = Cursor::new(data);
    let chunks: Vec<_> = chunker.stream(cursor).map(|c| c.unwrap()).collect();

    assert_eq!(chunks.len(), 3);
    for chunk in chunks {
        assert_eq!(chunk.length, max_size);
    }
}

struct SlowReader<R: std::io::Read> {
    inner: R,
}

impl<R: std::io::Read> std::io::Read for SlowReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Only read 1 byte at a time to test the Chunker's buffering/EOF logic
        if buf.is_empty() {
            return Ok(0);
        }
        self.inner.read(&mut buf[..1])
    }
}

#[test]
fn test_chunk_slow_reader() {
    let data: Vec<u8> = (0..500).map(|index| (index % 251) as u8).collect();
    let chunker = Chunker::new(64, 128, 256, Normalization::None);

    let slow_reader = SlowReader {
        inner: Cursor::new(data.clone()),
    };
    let chunks: Vec<_> = chunker.stream(slow_reader).map(|c| c.unwrap()).collect();

    let mut reconstructed = Vec::new();
    for chunk in chunks {
        reconstructed.extend_from_slice(&chunk.data);
    }

    assert_eq!(reconstructed, data);
}
