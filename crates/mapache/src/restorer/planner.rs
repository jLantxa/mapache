use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use crate::common::error::{MapacheError, Result};
use tokio::task::spawn_blocking;

use crate::{
    common::hash,
    fs::{self as repo_fs, node::Node},
    repository::index::MasterIndex,
    restorer::{Restorer, Strategy},
    utils::size,
};

impl Restorer {
    /// Checks if a node should be restored based on the current restoration strategy.
    pub(crate) async fn should_restore_node(
        &self,
        node: &Node,
        restore_path: &Path,
        index: Arc<MasterIndex>,
    ) -> Result<bool> {
        if !repo_fs::path_exists(restore_path).await {
            return Ok(true);
        }

        if node.is_file()
            && let Ok(local_metadata) = fs::symlink_metadata(restore_path)
        {
            let local_size = local_metadata.len();
            let local_mtime = local_metadata.modified().ok();

            if local_size == node.metadata.size {
                let mtime_matches = node
                    .metadata
                    .times_match(local_mtime, node.metadata.modified_time);

                if mtime_matches && !self.opts.verify {
                    return Ok(false);
                }

                let content_matches = match self
                    .verify_file_content(node, restore_path, index)
                    .await
                {
                    Ok(matches) => matches,
                    Err(e) => {
                        tracing::warn!(target: "restorer", "Could not verify file {:?}: {e}", restore_path);
                        false
                    }
                };
                if content_matches {
                    return Ok(false);
                }
            }
        }

        match self.opts.strategy {
            Strategy::Overwrite => Ok(true),
            Strategy::Skip => Ok(false),
            Strategy::Newer => {
                let local_metadata = fs::symlink_metadata(restore_path).map_err(|e| {
                    MapacheError::Internal(format!(
                        "failed to get metadata for local file {}: {e}",
                        restore_path.display()
                    ))
                })?;

                if let Some(repo_mtime) = node.metadata.modified_time {
                    let local_mtime = local_metadata.modified().map_err(|e| {
                        MapacheError::Internal(format!(
                            "failed to get modified time for local file {}: {e}",
                            restore_path.display()
                        ))
                    })?;

                    let local_size = local_metadata.len();
                    if local_mtime < repo_mtime {
                        return Ok(true);
                    }

                    if local_mtime == repo_mtime && local_size != node.metadata.size {
                        return Ok(true);
                    }

                    return Ok(false);
                }

                Ok(true)
            }
            Strategy::Fail => {
                if node.is_dir() {
                    return Ok(true);
                }
                Err(MapacheError::Internal(format!(
                    "target {} exists already",
                    restore_path.display()
                )))
            }
        }
    }

    /// Verifies that the content of a local file matches the blobs in the repository.
    async fn verify_file_content(
        &self,
        node: &Node,
        local_path: &Path,
        index: Arc<MasterIndex>,
    ) -> Result<bool> {
        let blobs = match &node.blobs {
            Some(b) => b.clone(),
            None => return Ok(true),
        };

        let local_path = local_path.to_path_buf();
        spawn_blocking(move || {
            let file = File::open(&local_path)?;
            let mut offset = 0;

            const VERIFY_BUFFER_SIZE: usize = size::MiB as usize;
            let mut buffer = vec![0u8; VERIFY_BUFFER_SIZE];

            for blob_id in blobs {
                let locator = index
                    .get_data(&blob_id)
                    .ok_or(MapacheError::NotInIndex(blob_id))?;

                let mut hasher = hash::Hasher::new();
                let mut remaining = locator.raw_length as u64;
                let mut blob_offset = offset;

                while remaining > 0 {
                    let to_read = (remaining as usize).min(VERIFY_BUFFER_SIZE);
                    let chunk = &mut buffer[..to_read];

                    #[cfg(unix)]
                    file.read_exact_at(chunk, blob_offset)?;
                    #[cfg(windows)]
                    {
                        let mut read_total = 0;
                        while read_total < to_read {
                            let n = file.seek_read(
                                &mut chunk[read_total..],
                                blob_offset + read_total as u64,
                            )?;
                            if n == 0 {
                                return Err(MapacheError::Internal(
                                    "unexpected EOF while reading blob for verification"
                                        .to_string(),
                                ));
                            }
                            read_total += n;
                        }
                    }

                    hasher.update(chunk);
                    let read_bytes = to_read as u64;
                    remaining -= read_bytes;
                    blob_offset += read_bytes;
                }

                let actual_id = hasher.finalize();
                if actual_id != blob_id {
                    return Ok(false);
                }
                offset += locator.raw_length as u64;
            }
            Ok(true)
        })
        .await
        .map_err(|e| MapacheError::Internal(format!("background task failed: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use crate::common::error::Result;
    use parking_lot::Mutex;
    use tempfile::tempdir;
    use zeroize::Zeroizing;

    use crate::backend::StorageBackend;
    use crate::backend::mock::MockBackend;
    use crate::common::defaults::TEST_REPO_CONFIG;
    use crate::fs::node::{Metadata, Node, NodeType};
    use crate::repository::index::MasterIndex;
    use crate::repository::repo::{Auth, Repository};
    use crate::restorer::{RestoreOptions, Restorer, Strategy};
    use crate::ui::events::noop_sender;

    fn auth() -> Auth {
        Auth {
            username: "test".to_string(),
            password: Zeroizing::new("test".to_string()),
        }
    }

    async fn create_restorer(strategy: Strategy, verify: bool) -> (Restorer, tempfile::TempDir) {
        let auth = auth();
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
        Repository::init(&auth, None, backend.clone())
            .await
            .unwrap();
        let (repo, _) = Repository::try_open_unlocked(&auth, None, backend, TEST_REPO_CONFIG)
            .await
            .unwrap();
        let tmp = tempdir().unwrap();
        let restorer = Restorer {
            repo,
            event_sender: noop_sender(),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            target_path: tmp.path().to_path_buf(),
            opts: RestoreOptions {
                strategy,
                strip_prefix: None,
                dry_run: false,
                quit_on_error: false,
                preallocate: false,
                verify,
                include: None,
                exclude: None,
                batch_size: None,
            },
            buffers: Arc::new(Mutex::new(VecDeque::new())),
        };
        (restorer, tmp)
    }

    fn file_node(size: u64) -> Node {
        Node {
            name: String::new(),
            node_type: NodeType::File,
            metadata: Metadata {
                size,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn dir_node() -> Node {
        Node {
            name: String::new(),
            node_type: NodeType::Directory,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_missing_path_always_restored() -> Result<()> {
        for strategy in [
            Strategy::Overwrite,
            Strategy::Skip,
            Strategy::Newer,
            Strategy::Fail,
        ] {
            let (restorer, _tmp) = create_restorer(strategy.clone(), false).await;
            let path = PathBuf::from("/does/not/exist");
            let index = Arc::new(MasterIndex::new());
            assert!(
                restorer
                    .should_restore_node(&file_node(100), &path, index)
                    .await?,
                "strategy {strategy:?} should restore missing paths"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_restores_existing() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Overwrite, false).await;
        let path = tmp.path().join("existing.txt");
        std::fs::write(&path, "data")?;
        let index = Arc::new(MasterIndex::new());
        assert!(
            restorer
                .should_restore_node(&file_node(999), &path, index)
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_skip_skips_existing() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Skip, false).await;
        let path = tmp.path().join("existing.txt");
        std::fs::write(&path, "data")?;
        let index = Arc::new(MasterIndex::new());
        assert!(
            !restorer
                .should_restore_node(&file_node(999), &path, index)
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_fail_errors_on_existing() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Fail, false).await;
        let path = tmp.path().join("existing.txt");
        std::fs::write(&path, "data")?;
        let index = Arc::new(MasterIndex::new());
        assert!(
            restorer
                .should_restore_node(&file_node(999), &path, index)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_fail_allows_existing_dir() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Fail, false).await;
        let path = tmp.path().join("existing_dir");
        std::fs::create_dir(&path)?;
        let index = Arc::new(MasterIndex::new());
        assert!(
            restorer
                .should_restore_node(&dir_node(), &path, index)
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_same_size_mtime_skips() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Overwrite, false).await;
        let path = tmp.path().join("existing.txt");
        std::fs::write(&path, "0123456789")?;
        let mtime = std::fs::symlink_metadata(&path)?.modified()?;

        let node = Node {
            name: "existing.txt".into(),
            node_type: NodeType::File,
            metadata: Metadata {
                size: 10,
                modified_time: Some(mtime),
                ..Default::default()
            },
            blobs: Some(vec![]),
            ..Default::default()
        };
        let index = Arc::new(MasterIndex::new());
        assert!(!restorer.should_restore_node(&node, &path, index).await?);
        Ok(())
    }
}
