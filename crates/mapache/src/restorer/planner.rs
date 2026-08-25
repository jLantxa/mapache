use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use tokio::task::spawn_blocking;

use crate::{
    common::{
        error::{MapacheError, Result},
        hash,
    },
    fs::{self as repo_fs, node::Node},
    repository::index::MasterIndex,
    restorer::{Restorer, Strategy},
    ui::events::{Event, RestoreEvent, emit_event},
    utils::size,
};

/// Describes the restore decision for a single node.
#[derive(Debug, Clone)]
pub(crate) enum RestorePlan {
    /// The node should not be restored (already matches or strategy says skip).
    Skip,
    /// The node must be fully restored (all blobs).
    FullRestore,
    /// Only the listed blob indices differ from local content and need download.
    /// All other blobs already match the local file.
    SelectiveRestore { changed_blobs: Vec<usize> },
}

impl Restorer {
    /// Decides how a node should be restored based on the current strategy and
    /// (when `verify` is enabled) per-blob content matching.
    pub(crate) async fn should_restore_node(
        &self,
        node: &Node,
        restore_path: &Path,
        index: Arc<MasterIndex>,
    ) -> Result<RestorePlan> {
        if !repo_fs::path_exists(restore_path).await {
            return Ok(RestorePlan::FullRestore);
        }

        match self.opts.strategy {
            Strategy::Skip => return Ok(RestorePlan::Skip),
            Strategy::Fail if !node.is_dir() => {
                return Err(MapacheError::Internal(format!(
                    "target {} exists already",
                    restore_path.display()
                )));
            }
            _ => {}
        }

        if self.opts.verify
            && node.is_file()
            && let Ok(local_metadata) = fs::symlink_metadata(restore_path)
            && local_metadata.len() == node.metadata.size
        {
            match self.verify_file_content(node, restore_path, index).await {
                Ok(changed) if changed.is_empty() => return Ok(RestorePlan::Skip),
                Ok(changed) => {
                    let total = node.blobs.as_ref().map_or(0, |b| b.len());
                    if changed.len() >= total {
                        return Ok(RestorePlan::FullRestore);
                    }
                    return Ok(RestorePlan::SelectiveRestore {
                        changed_blobs: changed,
                    });
                }
                Err(e) => {
                    tracing::warn!(target: "restorer", "Could not verify file {:?}: {e}", restore_path);
                    emit_event(
                        &self.event_sender,
                        Event::Restore(RestoreEvent::Warning(format!(
                            "Could not verify {}: {e}",
                            restore_path.display()
                        ))),
                    );
                }
            }
        }

        match self.opts.strategy {
            Strategy::Overwrite => Ok(RestorePlan::FullRestore),
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
                        return Ok(RestorePlan::FullRestore);
                    }

                    if local_mtime == repo_mtime && local_size != node.metadata.size {
                        return Ok(RestorePlan::FullRestore);
                    }

                    return Ok(RestorePlan::Skip);
                }

                Ok(RestorePlan::FullRestore)
            }
            Strategy::Fail => Ok(RestorePlan::FullRestore),
            Strategy::Skip => Ok(RestorePlan::Skip),
        }
    }

    /// Verifies that the content of a local file matches the blobs in the repository.
    /// Returns the list of blob indices whose content does NOT match the local file
    /// (i.e. blobs that need to be re-downloaded and written).
    /// An empty vector means the entire file content is identical.
    async fn verify_file_content(
        &self,
        node: &Node,
        local_path: &Path,
        index: Arc<MasterIndex>,
    ) -> Result<Vec<usize>> {
        let blobs = match &node.blobs {
            Some(b) => b.clone(),
            None => return Ok(Vec::new()),
        };

        let local_path = local_path.to_path_buf();
        spawn_blocking(move || {
            let file = File::open(&local_path)?;
            let mut offset = 0;
            let mut changed = Vec::new();

            const VERIFY_BUFFER_SIZE: usize = size::MiB as usize;
            let mut buffer = vec![0u8; VERIFY_BUFFER_SIZE];

            for (idx, blob_id) in blobs.iter().enumerate() {
                let locator = index
                    .get_data(blob_id)
                    .ok_or(MapacheError::NotInIndex(*blob_id))?;

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
                if actual_id != *blob_id {
                    changed.push(idx);
                }
                offset += locator.raw_length as u64;
            }
            Ok(changed)
        })
        .await
        .map_err(|e| MapacheError::Internal(format!("background task failed: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, path::PathBuf, sync::atomic::AtomicBool};

    use parking_lot::Mutex;
    use tempfile::tempdir;
    use zeroize::Zeroizing;

    use crate::{
        backend::{StorageBackend, mock::MockBackend},
        common::defaults::TEST_REPO_CONFIG,
        fs::node::{Metadata, NodeType},
        repository::repo::{Auth, Repository, THIS_REPOSITORY_VERSION},
        restorer::RestoreOptions,
        ui::events::noop_sender,
    };

    use super::*;

    fn auth() -> Auth {
        Auth {
            username: "test".to_string(),
            password: Zeroizing::new("test".to_string()),
        }
    }

    async fn create_restorer(strategy: Strategy, verify: bool) -> (Restorer, tempfile::TempDir) {
        let auth = auth();
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None)
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
            let index = Arc::new(MasterIndex::default());
            assert!(
                matches!(
                    restorer
                        .should_restore_node(&file_node(100), &path, index)
                        .await?,
                    RestorePlan::FullRestore
                ),
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
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer
                .should_restore_node(&file_node(999), &path, index)
                .await?,
            RestorePlan::FullRestore
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_skip_skips_existing() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Skip, false).await;
        let path = tmp.path().join("existing.txt");
        std::fs::write(&path, "data")?;
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer
                .should_restore_node(&file_node(999), &path, index)
                .await?,
            RestorePlan::Skip
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_fail_errors_on_existing() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Fail, false).await;
        let path = tmp.path().join("existing.txt");
        std::fs::write(&path, "data")?;
        let index = Arc::new(MasterIndex::default());
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
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer
                .should_restore_node(&dir_node(), &path, index)
                .await?,
            RestorePlan::FullRestore
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_same_size_mtime_skips() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Skip, false).await;
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
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer.should_restore_node(&node, &path, index).await?,
            RestorePlan::Skip
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite_ignores_mtime_match() -> Result<()> {
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
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer.should_restore_node(&node, &path, index).await?,
            RestorePlan::FullRestore
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_newer_restores_when_local_older() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Newer, false).await;
        let path = tmp.path().join("file.txt");
        std::fs::write(&path, "data")?;
        // Set local mtime to the past (1 hour ago)
        repo_fs::filetime::set_file_times(
            &path,
            repo_fs::filetime::FileTime::from_unix_time(1_000_000, 0),
            repo_fs::filetime::FileTime::from_unix_time(1_000_000, 0),
        )?;
        let local_mtime = std::fs::symlink_metadata(&path)?.modified()?;

        let node = Node {
            name: "file.txt".into(),
            node_type: NodeType::File,
            metadata: Metadata {
                size: 4,
                modified_time: Some(local_mtime + std::time::Duration::from_secs(3600)),
                ..Default::default()
            },
            blobs: Some(vec![]),
            ..Default::default()
        };
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer.should_restore_node(&node, &path, index).await?,
            RestorePlan::FullRestore
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_newer_skips_when_local_newer() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Newer, false).await;
        let path = tmp.path().join("file.txt");
        std::fs::write(&path, "data")?;
        let local_mtime = std::fs::symlink_metadata(&path)?.modified()?;

        let node = Node {
            name: "file.txt".into(),
            node_type: NodeType::File,
            metadata: Metadata {
                size: 4,
                modified_time: Some(local_mtime - std::time::Duration::from_secs(3600)),
                ..Default::default()
            },
            blobs: Some(vec![]),
            ..Default::default()
        };
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer.should_restore_node(&node, &path, index).await?,
            RestorePlan::Skip
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_newer_same_mtime_different_size_restores() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Newer, false).await;
        let path = tmp.path().join("file.txt");
        std::fs::write(&path, "data")?;
        let mtime = std::fs::symlink_metadata(&path)?.modified()?;

        let node = Node {
            name: "file.txt".into(),
            node_type: NodeType::File,
            metadata: Metadata {
                size: 999, // different from actual file size (4)
                modified_time: Some(mtime),
                ..Default::default()
            },
            blobs: Some(vec![]),
            ..Default::default()
        };
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer.should_restore_node(&node, &path, index).await?,
            RestorePlan::FullRestore
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_newer_same_mtime_same_size_skips() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Newer, false).await;
        let path = tmp.path().join("file.txt");
        std::fs::write(&path, "data")?;
        let mtime = std::fs::symlink_metadata(&path)?.modified()?;

        let node = Node {
            name: "file.txt".into(),
            node_type: NodeType::File,
            metadata: Metadata {
                size: 4,
                modified_time: Some(mtime),
                ..Default::default()
            },
            blobs: Some(vec![]),
            ..Default::default()
        };
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer.should_restore_node(&node, &path, index).await?,
            RestorePlan::Skip
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_newer_no_repo_mtime_restores() -> Result<()> {
        let (restorer, tmp) = create_restorer(Strategy::Newer, false).await;
        let path = tmp.path().join("file.txt");
        std::fs::write(&path, "data")?;

        let node = Node {
            name: "file.txt".into(),
            node_type: NodeType::File,
            metadata: Metadata {
                size: 4,
                modified_time: None, // no repo mtime
                ..Default::default()
            },
            blobs: Some(vec![]),
            ..Default::default()
        };
        let index = Arc::new(MasterIndex::default());
        assert!(matches!(
            restorer.should_restore_node(&node, &path, index).await?,
            RestorePlan::FullRestore
        ));
        Ok(())
    }
}
