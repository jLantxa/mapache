use async_trait::async_trait;

use crate::{
    backend::WriteContents,
    common::error::Result,
    common::{BlobType, ID, SaveID},
};

pub trait BlobSaver: Send + Sync {
    fn save_blob(
        &self,
        blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
    ) -> Result<ID>;
}

#[async_trait]
pub trait BlobLoader: Send + Sync {
    async fn load_blob(&self, id: &ID) -> Result<Vec<u8>>;

    /// Returns the decompressed (raw) length of the blob with the given ID
    /// without loading its data, if the loader can resolve it cheaply (e.g.
    /// from an index). Used to skip blobs that do not intersect a requested
    /// range without decrypting them. Loaders without index access return
    /// `Ok(None)`, in which case callers must fall back to loading.
    async fn blob_len(&self, _id: &ID) -> Result<Option<u64>> {
        Ok(None)
    }
}
