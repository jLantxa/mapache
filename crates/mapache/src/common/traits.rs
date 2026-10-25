use std::sync::OnceLock;

use async_trait::async_trait;

use crate::{
    backend::WriteContents,
    common::{
        BlobType, ID, SaveID,
        compression_policy::{CompressionHint, CompressionPolicy},
        error::Result,
    },
};

/// Returns the process-wide default compression policy (no patterns, probing
/// enabled). Used by [`BlobSaver`] implementors that do not carry their own.
pub(crate) fn default_compression_policy() -> &'static CompressionPolicy {
    static DEFAULT: OnceLock<CompressionPolicy> = OnceLock::new();
    DEFAULT.get_or_init(CompressionPolicy::with_probe)
}

pub trait BlobSaver: Send + Sync {
    fn save_blob(
        &self,
        blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
    ) -> Result<ID>;

    /// Compressibility policy used to decide which blobs to compress.
    /// Implementors that support per-blob compression override this.
    fn compression_policy(&self) -> &CompressionPolicy {
        default_compression_policy()
    }

    /// Reports whether a content sample is worth compressing.
    ///
    /// Implementors backed by a compression layer override this; the default
    /// assumes compressible, which makes probing a no-op.
    fn is_compressible(&self, _sample: &[u8]) -> bool {
        true
    }

    /// Resolves a `CompressionHint::Auto` decision from blob content.
    ///
    /// Returns a concrete `CompressionHint::Compress` or
    /// `CompressionHint::Store` when the implementor can decide from the
    /// sample *and* probing applies to it, or `None` when it cannot (no per-blob
    /// compression marker, compression disabled, or the policy has probing off).
    /// Callers then pass the hint through unchanged.
    ///
    /// This lets callers cache a decision across the blobs of one file instead
    /// of re-probing each one.
    fn probe_compression_hint(&self, _sample: &[u8]) -> Option<CompressionHint> {
        None
    }

    /// Saves a blob using an explicit per-blob compression hint.
    ///
    /// Implementors that do not support per-blob compression ignore the hint and
    /// fall back to [`BlobSaver::save_blob`].
    fn save_blob_with_hint(
        &self,
        hint: CompressionHint,
        blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
    ) -> Result<ID> {
        let _ = hint;
        self.save_blob(blob_type, data, save_id)
    }
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
