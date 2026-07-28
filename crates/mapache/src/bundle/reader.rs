use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use argon2::ParamsBuilder;
use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::Mutex;

use crate::{
    bundle::format::{
        BUNDLE_HEADER_SIZE, BUNDLE_KEY_LEN, BUNDLE_MAGIC_END, BUNDLE_MAGIC_START,
        BUNDLE_TRAILER_SIZE_LEN, BundleHeader, BundleIndex, BundleTrailer,
    },
    common::{
        ID,
        error::{MapacheError, Result},
        traits::BlobLoader,
    },
    fs::{
        node::Metadata,
        tree::{NodeDiff, Tree},
    },
    repository::storage::SecureStorage,
    restorer::node_restorer,
    ui::{
        self,
        events::{BackupEvent, Event, EventSender, RestoreEvent},
    },
    utils::{secure_join, stream::ReceiverStream},
};

pub struct BundleReader {
    file: Mutex<File>,
    storage: SecureStorage,
    index: BundleIndex,
    index_map: HashMap<ID, usize>,
    pub trailer: BundleTrailer,
}

#[async_trait]
impl BlobLoader for BundleReader {
    async fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        let idx = self
            .index_map
            .get(id)
            .ok_or_else(|| MapacheError::NotInIndex(*id))?;
        let entry = &self.index.entries[*idx];

        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(entry.offset))?;
        let mut encoded_data = vec![0u8; entry.length as usize];
        file.read_exact(&mut encoded_data)?;

        let data = self
            .storage
            .decode(&encoded_data)
            .map_err(|e| MapacheError::Crypto(format!("failed to decode blob data: {e}")))?;

        if data.len() != entry.raw_length as usize {
            return Err(MapacheError::Integrity(format!(
                "decoded blob length mismatch: expected {}, got {}",
                entry.raw_length,
                data.len()
            )));
        }

        Ok(data)
    }
}

impl BundleReader {
    pub fn open<P: AsRef<Path>>(path: P, password: &str) -> Result<Self> {
        let mut file = File::open(path)?;

        let mut header_bytes = vec![0u8; BUNDLE_HEADER_SIZE];
        file.read_exact(&mut header_bytes)
            .map_err(MapacheError::Io)?;
        let header = BundleHeader::from_binary(&header_bytes).map_err(|e| {
            MapacheError::Format(format!(
                "invalid bundle format: failed to parse header: {e}"
            ))
        })?;

        if header.magic != *BUNDLE_MAGIC_START {
            return Err(MapacheError::Format(
                "invalid bundle format: invalid magic start (not a mapache bundle)".to_string(),
            ));
        }

        let params = ParamsBuilder::new()
            .m_cost(header.argon2_m)
            .t_cost(header.argon2_t)
            .p_cost(header.argon2_p)
            .build()
            .map_err(|e| {
                MapacheError::Format(format!(
                    "invalid bundle format: Argon2 parameters are invalid: {}",
                    e
                ))
            })?;

        let key = SecureStorage::derive_key::<BUNDLE_KEY_LEN>(password, &header.salt, params)
            .map_err(|e| {
                MapacheError::Crypto(format!("failed to derive key from password: {e}"))
            })?;
        let storage = SecureStorage::new().with_key(&*key)?;

        file.seek(SeekFrom::End(-(BUNDLE_TRAILER_SIZE_LEN as i64)))?;
        let mut size_bytes = [0u8; BUNDLE_TRAILER_SIZE_LEN];
        file.read_exact(&mut size_bytes)?;
        let encrypted_trailer_size = u32::from_le_bytes(size_bytes);

        file.seek(SeekFrom::End(
            -(BUNDLE_TRAILER_SIZE_LEN as i64) - encrypted_trailer_size as i64,
        ))?;
        let mut encrypted_trailer = vec![0u8; encrypted_trailer_size as usize];
        file.read_exact(&mut encrypted_trailer)?;
        let decrypted_trailer = storage.decrypt(&encrypted_trailer).map_err(|e| {
            MapacheError::Crypto(format!(
                "failed to decrypt bundle trailer: incorrect password or corrupted data: {e}"
            ))
        })?;
        let trailer = BundleTrailer::from_binary(&decrypted_trailer).map_err(|e| {
            MapacheError::Format(format!(
                "invalid bundle format: failed to parse trailer: {e}"
            ))
        })?;

        if trailer.magic_end != *BUNDLE_MAGIC_END {
            return Err(MapacheError::Format(
                "invalid bundle format: invalid magic end".to_string(),
            ));
        }

        file.seek(SeekFrom::Start(trailer.index_offset))?;
        let mut encrypted_index = vec![0u8; trailer.index_len as usize];
        file.read_exact(&mut encrypted_index)?;
        let decrypted_index = storage.decrypt(&encrypted_index).map_err(|e| {
            MapacheError::Crypto(format!(
                "failed to decrypt bundle index: incorrect password or corrupted data: {e}"
            ))
        })?;
        let index = BundleIndex::from_binary(decrypted_index.as_ref()).map_err(|e| {
            MapacheError::Format(format!("invalid bundle format: failed to parse index: {e}"))
        })?;

        let mut index_map = HashMap::new();
        for (i, entry) in index.entries.iter().enumerate() {
            index_map.insert(entry.id, i);
        }

        Ok(Self {
            file: Mutex::new(file),
            storage,
            index,
            index_map,
            trailer,
        })
    }

    pub fn index(&self) -> &BundleIndex {
        &self.index
    }
}

pub async fn scan_bundle_tree<L>(loader: Arc<L>, tree_id: &ID) -> Result<(usize, u64)>
where
    L: BlobLoader + ?Sized + 'static,
{
    let mut total_items = 0;
    let mut total_bytes = 0;
    let mut stack = vec![*tree_id];

    while let Some(current_id) = stack.pop() {
        let data = loader
            .load_blob(&current_id)
            .await
            .map_err(|e| MapacheError::Repo(format!("failed to load tree: {e}")))?;
        let tree: Tree = serde_json::from_slice(&data)
            .map_err(|e| MapacheError::Repo(format!("failed to parse tree: {e}")))?;

        for node in tree.nodes {
            total_items += 1;
            if node.is_dir() {
                if let Some(subtree_id) = node.tree {
                    stack.push(subtree_id);
                }
            } else if node.is_file() {
                total_bytes += node.metadata.size;
            }
        }
    }
    Ok((total_items, total_bytes))
}

pub async fn extract_nodes_parallel<L>(
    loader: Arc<L>,
    root_id: &ID,
    destination: &Path,
    workers: usize,
    event_sender: EventSender,
) -> Result<()>
where
    L: BlobLoader + ?Sized + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel::<(PathBuf, crate::fs::node::Node)>(4096);
    let (dir_tx, dir_rx) = tokio::sync::mpsc::channel::<(PathBuf, Metadata)>(4096);

    let loader_clone = loader.clone();
    let dest_clone = destination.to_path_buf();
    let sender_clone = event_sender.clone();
    let root_id_val = *root_id;

    let walk_task = tokio::spawn(async move {
        let mut stack = vec![(dest_clone, root_id_val)];
        while let Some((current_dest, current_id)) = stack.pop() {
            let data = match loader_clone.load_blob(&current_id).await {
                Ok(d) => d,
                Err(e) => {
                    sender_clone(Event::Backup(BackupEvent::Error(format!(
                        "failed to load tree {}: {}",
                        current_id, e
                    ))));
                    continue;
                }
            };
            let tree: Tree = match serde_json::from_slice(&data) {
                Ok(t) => t,
                Err(e) => {
                    sender_clone(Event::Backup(BackupEvent::Error(format!(
                        "failed to parse tree {}: {}",
                        current_id, e
                    ))));
                    continue;
                }
            };

            for node in tree.nodes {
                let node_path = match secure_join(&current_dest, Path::new(&node.name)) {
                    Ok(p) => p,
                    Err(e) => {
                        sender_clone(Event::Backup(BackupEvent::Error(format!(
                            "skipping node {}: {}",
                            node.name, e
                        ))));
                        continue;
                    }
                };
                if node.is_dir() {
                    if let Err(e) = std::fs::create_dir_all(&node_path) {
                        sender_clone(Event::Backup(BackupEvent::Warning(format!(
                            "failed to create directory {}: {}",
                            node_path.display(),
                            e
                        ))));
                    }
                    if let Err(e) = dir_tx
                        .send((node_path.clone(), node.metadata.clone()))
                        .await
                    {
                        sender_clone(Event::Backup(BackupEvent::Warning(format!(
                            "internal channel error (dir): {}",
                            e
                        ))));
                    }
                    if let Some(subtree_id) = node.tree {
                        stack.push((node_path.clone(), subtree_id));
                    }
                }
                if let Err(e) = tx.send((node_path, node)).await {
                    sender_clone(Event::Backup(BackupEvent::Error(format!(
                        "internal channel error: {}",
                        e
                    ))));
                    break;
                }
            }
        }
    });

    let meta_sender = make_meta_sender();

    let process_future = async {
        let stream = ReceiverStream::new(rx);
        stream
            .for_each_concurrent(workers, |(path, node)| {
                let loader = loader.clone();
                let sender = event_sender.clone();
                let meta_sender = meta_sender.clone();
                async move {
                    sender(Event::Backup(BackupEvent::NodeProcessing {
                        path: path.clone(),
                        diff: NodeDiff::New,
                        size_hint: Some(node.metadata.size),
                    }));

                    if !node.is_file() {
                        if node.is_symlink()
                            && let Some(symlink_info) = &node.symlink_info
                        {
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::symlink;
                                if symlink(&symlink_info.target_path, &path).is_ok() {
                                    node_restorer::try_restore_node_metadata(
                                        &node.metadata,
                                        true,
                                        &path,
                                        &meta_sender,
                                    );
                                }
                            }

                            #[cfg(not(unix))]
                            let _ = symlink_info;
                        }
                        sender(Event::Backup(BackupEvent::NodeProcessed {
                            path: path.clone(),
                            diff: NodeDiff::New,
                            size_hint: Some(node.metadata.size),
                        }));
                        return;
                    }

                    let blobs = match &node.blobs {
                        Some(b) => b,
                        None => {
                            sender(Event::Backup(BackupEvent::NodeProcessed {
                                path: path.clone(),
                                diff: NodeDiff::New,
                                size_hint: Some(node.metadata.size),
                            }));
                            return;
                        }
                    };

                    let mut file = match std::fs::File::create(&path) {
                        Ok(f) => f,
                        Err(e) => {
                            sender(Event::Backup(BackupEvent::Error(format!(
                                "failed to create file {}: {}",
                                path.display(),
                                e
                            ))));
                            sender(Event::Backup(BackupEvent::NodeProcessed {
                                path: path.clone(),
                                diff: NodeDiff::New,
                                size_hint: Some(node.metadata.size),
                            }));
                            return;
                        }
                    };

                    let mut success = true;
                    for blob_id in blobs {
                        let data = match loader.load_blob(blob_id).await {
                            Ok(d) => d,
                            Err(e) => {
                                sender(Event::Backup(BackupEvent::Error(format!(
                                    "failed to load blob {} for {}: {}",
                                    blob_id,
                                    path.display(),
                                    e
                                ))));
                                success = false;
                                break;
                            }
                        };

                        use std::io::Write;
                        if let Err(e) = file.write_all(&data) {
                            sender(Event::Backup(BackupEvent::Error(format!(
                                "failed to write to {}: {}",
                                path.display(),
                                e
                            ))));
                            success = false;
                            break;
                        }
                        sender(Event::Backup(
                            BackupEvent::BytesProcessed(data.len() as u64),
                        ));
                    }

                    drop(file);
                    if success {
                        node_restorer::try_restore_node_metadata(
                            &node.metadata,
                            false,
                            &path,
                            &meta_sender,
                        );
                    }

                    sender(Event::Backup(BackupEvent::NodeProcessed {
                        path: path.clone(),
                        diff: NodeDiff::New,
                        size_hint: Some(node.metadata.size),
                    }));
                }
            })
            .await;
    };

    let _ = futures::join!(walk_task, process_future);

    let mut directories: Vec<(PathBuf, Metadata)> = Vec::new();
    let mut dir_rx = dir_rx;
    while let Some((path, meta)) = dir_rx.recv().await {
        directories.push((path, meta));
    }

    directories.sort_unstable_by_key(|(p, _)| std::cmp::Reverse(p.as_os_str().len()));
    for (p, meta) in directories {
        node_restorer::try_restore_node_metadata(&meta, false, &p, &meta_sender);
    }

    Ok(())
}

fn make_meta_sender() -> EventSender {
    Arc::new(|event: Event| {
        if let Event::Restore(RestoreEvent::Warning(ref msg)) = event {
            ui::cli::warning!("{}", msg);
        } else if let Event::Restore(RestoreEvent::Error(ref msg)) = event {
            ui::cli::error!("{}", msg);
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::common::ID;

    use super::*;

    struct MockLoader {
        blobs: HashMap<ID, Vec<u8>>,
    }

    impl MockLoader {
        fn from_trees(trees: Vec<(ID, Tree)>) -> Self {
            let blobs = trees
                .into_iter()
                .map(|(id, tree)| (id, serde_json::to_vec(&tree).unwrap()))
                .collect();
            Self { blobs }
        }
    }

    #[async_trait::async_trait]
    impl BlobLoader for MockLoader {
        async fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
            self.blobs
                .get(id)
                .cloned()
                .ok_or_else(|| MapacheError::NotInIndex(*id))
        }
    }

    #[tokio::test]
    async fn test_extract_rejects_path_traversal() {
        let traversal_name = "../../etc/passwd";
        let mut tree = Tree::default();
        tree.nodes.push(crate::fs::node::Node {
            name: traversal_name.to_string(),
            node_type: crate::fs::node::NodeType::File,
            metadata: Default::default(),
            blobs: None,
            symlink_info: None,
            tree: None,
        });

        let tree_id = ID::from_content(serde_json::to_vec(&tree).unwrap());
        let loader = Arc::new(MockLoader::from_trees(vec![(tree_id, tree)]));

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("extract");

        let events: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();
        let sender: EventSender = Arc::new(move |event| {
            events_clone.lock().push(event);
        });

        let result = extract_nodes_parallel(loader, &tree_id, &dest, 2, sender).await;
        assert!(result.is_ok());

        let traversal_detected = events
            .lock()
            .iter()
            .any(|e| matches!(e, Event::Backup(BackupEvent::Error(msg)) if msg.contains("Path traversal")));
        assert!(traversal_detected, "should emit error for path traversal");
    }

    #[tokio::test]
    async fn test_extract_allows_valid_paths() {
        let mut tree = Tree::default();
        tree.nodes.push(crate::fs::node::Node {
            name: "safe_file.txt".to_string(),
            node_type: crate::fs::node::NodeType::File,
            metadata: Default::default(),
            blobs: None,
            symlink_info: None,
            tree: None,
        });

        let tree_id = ID::from_content(serde_json::to_vec(&tree).unwrap());
        let loader = Arc::new(MockLoader::from_trees(vec![(tree_id, tree)]));

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("extract");

        let sender: EventSender = Arc::new(|_| {});

        let result = extract_nodes_parallel(loader, &tree_id, &dest, 2, sender).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_extract_rejects_absolute_path() {
        #[cfg(unix)]
        let absolute_name = "/etc/passwd";
        #[cfg(windows)]
        let absolute_name = "C:\\etc\\passwd";

        let mut tree = Tree::default();
        tree.nodes.push(crate::fs::node::Node {
            name: absolute_name.to_string(),
            node_type: crate::fs::node::NodeType::File,
            metadata: Default::default(),
            blobs: None,
            symlink_info: None,
            tree: None,
        });

        let tree_id = ID::from_content(serde_json::to_vec(&tree).unwrap());
        let loader = Arc::new(MockLoader::from_trees(vec![(tree_id, tree)]));

        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("extract");

        let events: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();
        let sender: EventSender = Arc::new(move |event| {
            events_clone.lock().push(event);
        });

        let result = extract_nodes_parallel(loader, &tree_id, &dest, 2, sender).await;
        assert!(result.is_ok());

        let error_detected = events
            .lock()
            .iter()
            .any(|e| matches!(e, Event::Backup(BackupEvent::Error(msg)) if msg.contains("cannot be absolute")));
        assert!(error_detected, "should emit error for absolute path");
    }
}
