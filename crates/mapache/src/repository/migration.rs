//! Migration utilities for upgrading repository format versions.
//!
//! All items in this module are temporary and should be removed when v1 is deprecated.

use crate::backend::{Handle, StorageBackend};
use crate::common::{
    ContentIdType, ID,
    error::{MapacheError, Result},
};
use crate::repository::packer::{PackedBlobDescriptor, Packer};
use crate::repository::repo::Repository;
use crate::repository::storage::SecureStorage;

/// Re-encrypt a single pack from `old_nonce_at_end` to `new_nonce_at_end` position.
///
/// Reads the pack, decrypts every blob with the old nonce layout, re-encrypts
/// with the new layout, and re-encrypts the footer. On success returns
/// `(new_id, descriptors)` where `descriptors` only includes non-padding entries
/// with correct offsets for the new data section.
///
/// The data section only contains non-padding blobs (padding blobs in the footer
/// have random offset/length — they are noise only, no data in the pack's data
/// section). The footer is re-encrypted with the new nonce position.
pub async fn re_encrypt_pack(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    old_pack_id: &ID,
    old_nonce_at_end: bool,
    new_nonce_at_end: bool,
) -> Result<(ID, Vec<PackedBlobDescriptor>)> {
    let old_path = repo.get_path(ContentIdType::Pack, old_pack_id);
    let old_handle = Handle::new(&old_path);

    // Read footer length (last 4 bytes, unencrypted).
    let footer_len_bytes: [u8; 4] = backend
        .read(&old_handle, -4, 4)
        .await?
        .as_slice()
        .try_into()
        .map_err(|e: std::array::TryFromSliceError| {
            MapacheError::Format(format!("invalid footer length bytes: {e}"))
        })?;
    let encoded_footer_length = u32::from_le_bytes(footer_len_bytes) as usize;

    // Read the full pack.
    let pack_data = backend.read(&old_handle, 0, 0).await?;

    let total_len = pack_data.len();
    let data_section_end = total_len - 4 - encoded_footer_length;

    // Parse footer with old nonce position to get descriptors (non-padding only, correct offsets).
    let descriptors = Packer::parse_footer(secure_storage, &pack_data, old_nonce_at_end)?;

    tracing::debug!(target: "migrate", "Pack {}: {} blobs, data_section={} bytes, footer={} bytes",
        old_pack_id.to_short_hex(8), descriptors.len(), data_section_end, encoded_footer_length);

    // Re-encrypt each non-padding blob and build the new data section.
    let mut new_data = Vec::with_capacity(data_section_end);
    for desc in &descriptors {
        let start = desc.offset as usize;
        let end = start + desc.length as usize;
        let blob_encrypted = &pack_data[start..end];
        let re_encrypted =
            secure_storage.re_encrypt(blob_encrypted, old_nonce_at_end, new_nonce_at_end)?;
        new_data.extend_from_slice(&re_encrypted);
    }

    // Re-encrypt the existing footer with the new nonce position.
    let footer_encrypted = &pack_data[data_section_end..total_len - 4];
    let re_encrypted_footer =
        secure_storage.re_encrypt(footer_encrypted, old_nonce_at_end, new_nonce_at_end)?;

    // Assemble the new pack: [re-encrypted blobs | re-encrypted footer | footer length].
    let mut new_pack = new_data;
    new_pack.extend_from_slice(&re_encrypted_footer);
    new_pack.extend_from_slice(&footer_len_bytes);

    let new_id = ID::from_content(&new_pack);

    // Write the new pack.
    let new_path = repo.get_path(ContentIdType::Pack, &new_id);
    let new_handle = Handle::new(&new_path);
    backend.write(&new_handle, new_pack.into()).await?;

    // NOTE: Old pack is NOT deleted here. Caller is responsible for cleanup
    // after the manifest is updated, ensuring atomic migration.

    // Descriptors already have correct offsets for the new data section
    // (since data section layout is preserved — only non-padding blobs).
    Ok((new_id, descriptors))
}

/// Re-encrypt a standalone file (snapshot, index, etc.) from one nonce position to another.
///
/// The file ID is the content hash of the encrypted bytes, so re-encryption
/// produces a new ID. Returns `new_id`.
pub async fn re_encrypt_file(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    file_type: ContentIdType,
    old_id: &ID,
    old_nonce_at_end: bool,
    new_nonce_at_end: bool,
) -> Result<ID> {
    let old_path = repo.get_path(file_type, old_id);
    let data = backend.read(&Handle::new(&old_path), 0, 0).await?;

    let re_encrypted = secure_storage.re_encrypt(&data, old_nonce_at_end, new_nonce_at_end)?;

    let new_id = ID::from_content(&re_encrypted);
    let new_path = repo.get_path(file_type, &new_id);
    backend
        .write(&Handle::new(&new_path), re_encrypted.into())
        .await?;

    // NOTE: Old file is NOT deleted here. Caller is responsible for cleanup
    // after the manifest is updated, ensuring atomic migration.

    Ok(new_id)
}
