use proptest::prelude::*;

use crate::common::ID;

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
        use crate::repository::snapshot::Snapshot;
        use chrono::{TimeZone, Local};

        let snapshot = Snapshot {
            timestamp: Local.timestamp_opt(timestamp_secs, 0).unwrap(),
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
        use crate::bundle::format::{BundleHeader, BUNDLE_MAGIC_START};

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
    ) {
        use crate::bundle::format::{BundleTrailer, BUNDLE_MAGIC_END};

        let trailer = BundleTrailer {
            root_tree: root_id,
            index_offset,
            index_len,
            manifest_offset,
            manifest_len,
            magic_end: *BUNDLE_MAGIC_END,
        };
        let bytes = trailer.to_binary();
        let restored = BundleTrailer::from_binary(&bytes)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        prop_assert_eq!(trailer.root_tree, restored.root_tree);
        prop_assert_eq!(trailer.index_offset, restored.index_offset);
        prop_assert_eq!(trailer.index_len, restored.index_len);
        prop_assert_eq!(trailer.manifest_offset, restored.manifest_offset);
        prop_assert_eq!(trailer.manifest_len, restored.manifest_len);
        prop_assert_eq!(trailer.magic_end, restored.magic_end);
    }

    #[test]
    fn bundle_index_entry_roundtrip(
        id in arb_id(),
        blob_type in prop_oneof![
            Just(crate::common::BlobType::Data),
            Just(crate::common::BlobType::Tree),
            Just(crate::common::BlobType::Zero),
            Just(crate::common::BlobType::Padding),
        ],
        compressed in prop::bool::ANY,
        offset in 0u64..u64::MAX,
        length in 0u32..u32::MAX,
        raw_length in 0u32..u32::MAX,
    ) {
        use crate::bundle::format::BundleIndexEntry;

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
        use crate::repository::storage::SecureStorage;

        let ss = SecureStorage::new();
        let encoded = ss.encode(&data)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        let decoded = ss.decode(&encoded)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        prop_assert_eq!(data, decoded.as_slice());
    }

    #[test]
    fn storage_encode_decode_with_key(data in prop::collection::vec(any::<u8>(), 0..10_000)) {
        use crate::repository::storage::SecureStorage;

        let key = [0x42u8; 32];
        let ss = SecureStorage::new().with_key(&key).unwrap();
        let encoded = ss.encode(&data)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        let decoded = ss.decode(&encoded)
            .map_err(|e| proptest::test_runner::TestCaseError::Fail(e.to_string().into()))?;
        prop_assert_eq!(data, decoded.as_slice());
    }

}
