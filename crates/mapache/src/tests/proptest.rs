use chrono::TimeZone;
use proptest::prelude::*;

use crate::{
    bundle::format::{
        BUNDLE_MAGIC_END, BUNDLE_MAGIC_START, BundleHeader, BundleIndexEntry, BundleTrailer,
    },
    common::{BlobType, ID},
    ecc::ecc_decode,
    repository::{
        index::{
            IndexFile, IndexFileBlob, IndexFilePack, deserialize_index_binary,
            serialize_index_binary,
        },
        packer::{PackedBlobDescriptor, Packer},
        snapshot::Snapshot,
        storage::SecureStorage,
    },
};

fn arb_id() -> impl Strategy<Value = ID> {
    prop::array::uniform32(0u8..=255u8).prop_map(ID::from_bytes)
}

proptest! {
    #[test]
    fn id_hex_roundtrip(id_bytes in prop::array::uniform32(0u8..=255u8)) {
        let id = ID::from_bytes(id_bytes);
        let hex = id.to_hex();
        let parsed = ID::from_hex(&hex).map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        prop_assert_eq!(id, parsed);
    }

    #[test]
    fn id_short_hex_roundtrip(id_bytes in prop::array::uniform32(0u8..=255u8), len in 1usize..=32) {
        let id = ID::from_bytes(id_bytes);
        let short = id.to_short_hex(len);
        prop_assert_eq!(short.len(), len * 2);
        // The short hex should be a prefix of the full hex
        let full_hex = id.to_hex();
        prop_assert!(full_hex.starts_with(&short));
    }

    #[test]
    fn snapshot_json_roundtrip(
        timestamp_secs in 0i64..4_000_000_000i64,
        hostname in "[a-z]{1,10}",
        username in "[a-z]{1,10}",
    ) {

        let snapshot = Snapshot {
            timestamp: chrono::Local.timestamp_opt(timestamp_secs, 0).unwrap(),
            parent: None,
            tree: ID::from_content(b"test-tree"),
            root: "/".into(),
            paths: vec!["/data".into()],
            hostname: Some(hostname),
            username: Some(username),
            version: Some("0.5.2".to_string()),
            tags: Default::default(),
            description: None,
            summary: Default::default(),
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        let parsed: Snapshot = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(snapshot.tree, parsed.tree);
        prop_assert_eq!(snapshot.root, parsed.root);
    }

    #[test]
    fn bundle_header_roundtrip(
        salt in prop::array::uniform32(0u8..=255u8),
        argon2_t in 1u32..100,
        argon2_m in 1u32..1000,
        argon2_p in 1u32..4,
    ) {

        let header = BundleHeader {
            magic: *BUNDLE_MAGIC_START,
            version: 2,
            salt,
            argon2_t,
            argon2_m,
            argon2_p,
        };

        let bytes = header.to_binary();
        let restored = BundleHeader::from_binary(&bytes)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;

        prop_assert_eq!(header.magic, restored.magic);
        prop_assert_eq!(header.version, restored.version);
        prop_assert_eq!(header.salt, restored.salt);
        prop_assert_eq!(header.argon2_t, restored.argon2_t);
        prop_assert_eq!(header.argon2_m, restored.argon2_m);
        prop_assert_eq!(header.argon2_p, restored.argon2_p);
    }

    #[test]
    fn bundle_trailer_roundtrip(
        root_id in arb_id(),
        index_offset in 0u64..u64::MAX,
        index_len in 0u32..u32::MAX,
        manifest_offset in 0u64..u64::MAX,
        manifest_len in 0u32..u32::MAX,
        has_ecc in prop::bool::ANY,
    ) {

        let (ecc_offset, ecc_len) = if has_ecc {
            (index_offset.saturating_sub(1), 81u32)
        } else {
            (0, 0)
        };

        let trailer = BundleTrailer {
            root_tree: root_id,
            index_offset,
            index_len,
            manifest_offset,
            manifest_len,
            ecc_offset,
            ecc_len,
            magic_end: *BUNDLE_MAGIC_END,
        };
        let bytes = trailer.to_binary(has_ecc);
        let restored = BundleTrailer::from_binary_auto(&bytes)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        prop_assert_eq!(trailer.root_tree, restored.root_tree);
        prop_assert_eq!(trailer.index_offset, restored.index_offset);
        prop_assert_eq!(trailer.index_len, restored.index_len);
        prop_assert_eq!(trailer.manifest_offset, restored.manifest_offset);
        prop_assert_eq!(trailer.manifest_len, restored.manifest_len);
        prop_assert_eq!(trailer.magic_end, restored.magic_end);
        prop_assert_eq!(trailer.ecc_offset, restored.ecc_offset);
        prop_assert_eq!(trailer.ecc_len, restored.ecc_len);
    }

    #[test]
    fn bundle_index_entry_roundtrip(
        id in arb_id(),
        blob_type in prop_oneof![
            Just(BlobType::Data),
            Just(BlobType::Tree),
            Just(BlobType::Zero),
            Just(BlobType::Padding),
        ],
        compressed in prop::bool::ANY,
        offset in 0u64..u64::MAX,
        length in 0u32..u32::MAX,
        raw_length in 0u32..u32::MAX,
    ) {

        let entry = BundleIndexEntry { id, blob_type, compressed, offset, length, raw_length };
        let bytes = entry.to_binary();
        prop_assert_eq!(bytes.len(), 49); // 32 + 1 + 8 + 4 + 4
        let restored = BundleIndexEntry::from_binary(&bytes)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        prop_assert_eq!(entry.id, restored.id);
        prop_assert_eq!(entry.blob_type as u8, restored.blob_type as u8);
        prop_assert_eq!(entry.compressed, restored.compressed);
        prop_assert_eq!(entry.offset, restored.offset);
        prop_assert_eq!(entry.length, restored.length);
        prop_assert_eq!(entry.raw_length, restored.raw_length);
    }

    #[test]
    fn storage_encode_decode_no_key(data in prop::collection::vec(any::<u8>(), 0..10_000)) {


        let ss = SecureStorage::new();
        let encoded = ss.encode(&data)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        let decoded = ss.decode(&encoded)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        prop_assert_eq!(data, decoded.as_slice());
    }

    #[test]
    fn storage_encode_decode_with_key(data in prop::collection::vec(any::<u8>(), 0..10_000)) {

        let key = [0x42u8; 32];
        let ss = SecureStorage::new().with_key(&key).unwrap();
        let encoded = ss.encode(&data)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        let decoded = ss.decode(&encoded)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        prop_assert_eq!(data, decoded.as_slice());
    }

    #[test]
    fn pack_footer_arbitrary_input_never_panics(input in prop::collection::vec(any::<u8>(), 0..3500)) {

        let ss = SecureStorage::new();
        let _ = Packer::parse_footer(&ss, &input, true, 2);
    }

    #[test]
    fn pack_footer_corruption_is_rejected(positions in prop::collection::vec(0usize..3500, 1..10)) {

        let key = [0x42u8; 32];
        let ss = SecureStorage::new().with_key(&key).unwrap();

        // Exactly FOOTER_BLOB_MULTIPLE descriptors: no padding is appended, so
        // the generated footer is fully deterministic.
        let mut descriptors: Vec<PackedBlobDescriptor> = (0..64u32)
            .map(|i| PackedBlobDescriptor {
                id: ID::from_content([i as u8]),
                blob_type: BlobType::Data,
                offset: i,
                length: i + 1,
                raw_length: i + 1,
                compressed: true,
            })
            .collect();
        let plain_footer = Packer::generate_footer(&mut descriptors);
        let encoded = ss.encode(&plain_footer).unwrap();
        let mut footer_data = encoded.clone();
        footer_data.extend_from_slice(&(encoded.len() as u32).to_le_bytes());

        // Flip whole bytes so every mutation is guaranteed to change the input.
        for pos in positions {
            let idx = pos % footer_data.len();
            footer_data[idx] ^= 0xFF;
        }

        // Any changed byte breaks either the AEAD tag or the footer-length
        // framing, so a corrupted footer must always be rejected.
        let err = Packer::parse_footer(&ss, &footer_data, true, 2)
            .expect_err("corrupted pack footer must be rejected");
        prop_assert!(!(err.to_string().is_empty()));
    }

    #[test]
    fn ecc_decode_arbitrary_input_never_panics(
        data in prop::collection::vec(any::<u8>(), 0..4096),
        ecc_payload in prop::collection::vec(any::<u8>(), 0..4096),
    ) {

        let _ = ecc_decode(&data, &ecc_payload);
    }

    #[test]
    fn index_binary_arbitrary_input_never_panics(input in prop::collection::vec(any::<u8>(), 0..1000)) {

        let _ = deserialize_index_binary(&input);
    }

    #[test]
    fn index_binary_corruption_is_faithful_or_rejected(positions in prop::collection::vec(0usize..400, 1..10)) {

        let mut packs = Vec::new();
        for pack_id in 0..3u8 {
            let blobs = (0..(pack_id + 1)).map(|i| IndexFileBlob {
                id: ID::from_content([pack_id, i]),
                blob_type: BlobType::Data,
                offset: i as u32,
                length: 10 + i as u32,
                raw_length: 20 + i as u32,
                compressed: true,
            })
            .collect();
            packs.push(IndexFilePack { id: ID::from_content([pack_id]), blobs });
        }
        let index = IndexFile { packs };

        let mut bytes = serialize_index_binary(&index);
        for pos in positions {
            let idx = pos % bytes.len();
            bytes[idx] ^= 0xFF;
        }

        if let Ok(parsed) = deserialize_index_binary(&bytes) {
            // A successful parse must reproduce exactly the bytes it consumed;
            // anything else would mean silent corruption.
            let reserialized = serialize_index_binary(&parsed);
            prop_assert!(bytes.starts_with(&reserialized));
        }
    }

    #[test]
    fn secure_storage_decode_corruption_never_silently_succeeds(
        data in prop::collection::vec(any::<u8>(), 0..4096),
        positions in prop::collection::vec(0usize..5000, 1..10),
    ) {

        let key = [0x42u8; 32];
        let ss = SecureStorage::new().with_key(&key).unwrap();
        let mut encoded = ss.encode(&data).unwrap();

        for pos in positions {
            let idx = pos % encoded.len();
            encoded[idx] ^= 0xFF;
        }

        if let Ok(decoded) = ss.decode(&encoded) {
            prop_assert_eq!(&decoded[..], &data[..]);
        }
    }

}
