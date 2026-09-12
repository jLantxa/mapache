//! Migration utilities for upgrading repository format versions.
//!
//! All items in this module are temporary and should be removed when v1 is deprecated.
// TODO(v1-removal): Remove this entire module.

use crate::{
    archiver::processor::is_all_zero,
    backend::{Handle, StorageBackend},
    common::{
        BlobType, ContentIdType, ID,
        error::{MapacheError, Result},
    },
    repository::{
        packer::{PackedBlobDescriptor, Packer},
        repo::Repository,
        storage::SecureStorage,
    },
};

/// Re-encrypt a single pack from `old_nonce_at_end` to `new_nonce_at_end` position.
///
/// v2 uses the same JSON tree serialization as v1, so tree blobs are re-encrypted
/// in place and the tree hierarchy is left untouched.
pub(crate) async fn re_encrypt_pack(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    old_pack_id: &ID,
    old_nonce_at_end: bool,
    new_nonce_at_end: bool,
) -> Result<(ID, Vec<PackedBlobDescriptor>)> {
    let old_path = repo.get_path(ContentIdType::Pack, old_pack_id);
    let old_handle = Handle::new(&old_path);

    let pack_data = backend.read(&old_handle, 0, 0).await?;

    if pack_data.len() < 4 {
        return Err(MapacheError::Format(format!(
            "pack {} is too small ({} bytes), expected at least 4 bytes for footer length",
            old_pack_id.to_short_hex(8),
            pack_data.len()
        )));
    }

    let footer_len_bytes: [u8; 4] = pack_data[pack_data.len() - 4..].try_into().map_err(
        |e: std::array::TryFromSliceError| {
            MapacheError::Format(format!("invalid footer length bytes: {e}"))
        },
    )?;
    let encoded_footer_length = u32::from_le_bytes(footer_len_bytes) as usize;

    let total_len = pack_data.len();
    if total_len < 4 + encoded_footer_length {
        return Err(MapacheError::Format(format!(
            "pack {} footer length ({}) exceeds pack size ({})",
            old_pack_id.to_short_hex(8),
            encoded_footer_length,
            total_len
        )));
    }
    let data_section_end = total_len - 4 - encoded_footer_length;

    let mut descriptors = Packer::parse_footer(secure_storage, &pack_data, old_nonce_at_end, 1)?;

    tracing::debug!(target: "migrate", "Pack {}: {} blobs, data_section={} bytes, footer={} bytes",
        old_pack_id.to_short_hex(8), descriptors.len(), data_section_end, encoded_footer_length);

    let mut new_data = Vec::with_capacity(data_section_end);
    let mut new_offset = 0u32;

    for desc in &mut descriptors {
        if matches!(desc.blob_type, BlobType::Padding) {
            continue;
        }

        let start = desc.offset as usize;
        let end = start + desc.length as usize;
        if end < start || end > data_section_end {
            return Err(MapacheError::Format(format!(
                "pack {} descriptor for blob {} is out of bounds: offset {} + length {} exceeds data section ({} bytes)",
                old_pack_id.to_short_hex(8),
                desc.id.to_short_hex(8),
                desc.offset,
                desc.length,
                data_section_end
            )));
        }
        let blob_encrypted = &pack_data[start..end];

        let plaintext = secure_storage
            .decrypt_inner(blob_encrypted, old_nonce_at_end)?
            .into_owned();

        if is_all_zero(&plaintext) {
            desc.blob_type = BlobType::Zero;
            desc.offset = 0;
            desc.length = 0;
        } else {
            let re_encrypted =
                secure_storage.re_encrypt(blob_encrypted, old_nonce_at_end, new_nonce_at_end)?;
            desc.offset = new_offset;
            desc.length = re_encrypted.len() as u32;
            new_offset = new_offset.checked_add(desc.length).ok_or_else(|| {
                MapacheError::Format(format!(
                    "pack {} exceeds 4 GiB while re-encrypting (offset overflow at blob {})",
                    old_pack_id.to_short_hex(8),
                    desc.id.to_short_hex(8)
                ))
            })?;
            new_data.extend_from_slice(&re_encrypted);
        }
    }

    let mut footer_descriptors: Vec<_> = descriptors
        .iter()
        .filter(|d| !matches!(d.blob_type, BlobType::Padding))
        .cloned()
        .collect();
    let footer_bytes = Packer::generate_footer(&mut footer_descriptors);
    let mut ctx = secure_storage.get_encoding_context()?;
    let new_footer =
        secure_storage.encode_with_nonce_position(&mut ctx, &footer_bytes, new_nonce_at_end)?;
    let new_footer_len = u32::try_from(new_footer.len()).map_err(|_| {
        MapacheError::Internal(format!(
            "rebuilt pack footer too large ({} bytes)",
            new_footer.len()
        ))
    })?;
    let footer_len_bytes = new_footer_len.to_le_bytes();

    let mut new_pack = new_data;
    new_pack.extend_from_slice(&new_footer);
    new_pack.extend_from_slice(&footer_len_bytes);

    let new_id = ID::from_content(&new_pack);

    let new_path = repo.get_path(ContentIdType::Pack, &new_id);
    let new_handle = Handle::new(&new_path);
    backend.write(&new_handle, new_pack.into()).await?;

    Ok((new_id, descriptors))
}

/// Parse a pack's footer and return its blob descriptors.
///
/// Used by dry runs to report accurate blob counts; also validates that the
/// pack footer can be read and decrypted.
pub(crate) async fn read_pack_descriptors(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    pack_id: &ID,
    nonce_at_end: bool,
) -> Result<Vec<PackedBlobDescriptor>> {
    Packer::parse_pack_footer(repo, backend, secure_storage, pack_id, nonce_at_end).await
}

/// Re-encrypt a standalone file (snapshot, index, etc.).
pub(crate) async fn re_encrypt_file(
    params: &ReEncryptParams<'_>,
    file_type: ContentIdType,
    old_id: &ID,
    extension: Option<&str>,
) -> Result<ID> {
    let old_path = params
        .repo
        .get_path(file_type, old_id)
        .with_extension(extension.unwrap_or_default());
    let data = params.backend.read(&Handle::new(&old_path), 0, 0).await?;
    let re_encrypted = params.secure_storage.re_encrypt(
        &data,
        params.old_nonce_at_end,
        params.new_nonce_at_end,
    )?;
    let new_id = ID::from_content(&re_encrypted);
    let new_path = params
        .repo
        .get_path(file_type, &new_id)
        .with_extension(extension.unwrap_or_default());
    let new_handle = Handle::new(&new_path);
    params
        .backend
        .write(&new_handle, re_encrypted.into())
        .await?;
    Ok(new_id)
}

/// Parameters for re-encryption during migration.
pub(crate) struct ReEncryptParams<'a> {
    pub(crate) repo: &'a Repository,
    pub(crate) backend: &'a dyn StorageBackend,
    pub(crate) secure_storage: &'a SecureStorage,
    pub(crate) old_nonce_at_end: bool,
    pub(crate) new_nonce_at_end: bool,
}
