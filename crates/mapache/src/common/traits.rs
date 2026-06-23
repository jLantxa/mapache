use anyhow::Result;
use async_trait::async_trait;

use crate::{
    backend::WriteContents,
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
}
