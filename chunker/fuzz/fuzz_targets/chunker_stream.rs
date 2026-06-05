#![cfg_attr(fuzzing, no_main)]
use std::io::Cursor;

use chunker::{Chunker, Normalization};
use libfuzzer_sys::fuzz_target;

#[cfg(not(fuzzing))]
fn main() {
    println!("This is a fuzzer target. Run it with cargo-fuzz.");
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }

    // Use the first 4 bytes to vary chunker parameters
    let min_size = (data[0] as usize % 1024).max(64);
    let normal_size = (data[1] as usize % 8192).max(min_size);
    let max_size = (data[2] as usize % 32768).max(normal_size);
    let norm = match data[3] % 4 {
        0 => Normalization::None,
        1 => Normalization::L1,
        2 => Normalization::L2,
        _ => Normalization::L3,
    };

    let rest = &data[4..];
    let chunker = Chunker::new(min_size, normal_size, max_size, norm);
    let stream = chunker.stream(Cursor::new(rest));

    let mut reconstructed = Vec::with_capacity(rest.len());
    let mut last_offset = 0;

    for res in stream {
        match res {
            Ok(chunk) => {
                assert_eq!(chunk.offset, last_offset, "Chunk offset mismatch");
                assert_eq!(
                    chunk.length,
                    chunk.data.len(),
                    "Chunk length field mismatch"
                );

                reconstructed.extend_from_slice(&chunk.data);
                last_offset += chunk.length;

                // All chunks except the last one must be >= min_size
                // (Unless the total data is less than min_size)
                if last_offset < rest.len() {
                    assert!(
                        chunk.length >= min_size,
                        "Chunk too small: {} < {}",
                        chunk.length,
                        min_size
                    );
                }

                assert!(
                    chunk.length <= max_size,
                    "Chunk too large: {} > {}",
                    chunk.length,
                    max_size
                );
            }
            Err(e) => panic!("Stream error: {}", e),
        }
    }

    assert_eq!(reconstructed, rest, "Reconstructed data mismatch");
    assert_eq!(last_offset, rest.len(), "Total processed bytes mismatch");
});
