use crate::backend::WriteContents;
use crate::mapache::{BlobType, ID, SaveID};
use anyhow::Result;

pub trait BlobSaver: Send + Sync {
    fn save_blob(
        &self,
        blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
        compress: bool,
    ) -> Result<ID>;
}

#[async_trait::async_trait]
pub trait BlobLoader: Send + Sync {
    async fn load_blob(&self, id: &ID) -> Result<Vec<u8>>;
}
