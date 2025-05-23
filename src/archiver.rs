// [backup] is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{
    collections::BTreeMap,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Local;
use fastcdc::v2020::{Normalization, StreamCDC};

use crate::{
    backup::{self, ObjectId, ObjectType},
    cli,
    repository::{
        RepositoryBackend,
        snapshot::Snapshot,
        tree::{
            FSNodeStreamer, Node, NodeDiff, NodeDiffStreamer, NodeType, SerializedNodeStreamer,
            StreamNode, Tree,
        },
    },
    ui::commit::CommitProgressReporter,
    utils::{self, Hash},
};

/// Represents a directory node that is being built bottom-up during the commit process.
/// It holds the directory's own node information (if available), the collected child nodes,
/// and the number of children expected from the stream.
#[derive(Debug)]
struct PendingTree {
    pub node: Option<Node>,
    pub children: BTreeMap<String, Node>,
    pub num_expected_children: isize,
}

impl PendingTree {
    ///  Returns true if this directory node is still waiting to receive children
    pub fn is_pending(&self) -> bool {
        self.num_expected_children < 0
            || (self.children.len() as isize) < self.num_expected_children
    }
}

pub struct Archiver {}

impl Archiver {
    /// Orchestrates the backup commit process, building a new snapshot of the source paths.
    ///
    /// This implementation utilizes a multi-threaded, channel-based architecture to manage
    /// the workflow.Dedicated threads handle generating the difference stream, processing
    /// individual file and directory changes, and serializing the resulting tree structure
    /// bottom-up to create the final snapshot.
    pub fn snapshot(
        repo: Arc<dyn RepositoryBackend>,
        absolute_source_paths: Vec<PathBuf>,
        commit_root_path: PathBuf,
        parent_snapshot: Option<Snapshot>,
        progress_reporter: Option<Arc<CommitProgressReporter>>,
    ) -> Result<Snapshot> {
        // Extract parent snapshot tree id
        let parent_tree_id: Option<ObjectId> = match &parent_snapshot {
            None => None,
            Some(snapshot) => Some(snapshot.tree.clone()),
        };

        // Create streamers
        let fs_streamer = match FSNodeStreamer::from_paths(absolute_source_paths.clone()) {
            Ok(stream) => stream,
            Err(e) => bail!("Failed to create FSNodeStreamer: {:?}", e.to_string()),
        };
        let previous_tree_streamer =
            SerializedNodeStreamer::new(repo.clone(), parent_tree_id, commit_root_path.clone())?;

        let num_threads = std::cmp::max(1, num_cpus::get() / 2);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()?;

        // Channels
        let (diff_tx, diff_rx) = crossbeam_channel::bounded::<(
            PathBuf,
            Option<StreamNode>,
            Option<StreamNode>,
            NodeDiff,
        )>(2 * num_threads);
        let (process_item_tx, process_item_rx) =
            crossbeam_channel::bounded::<(PathBuf, StreamNode)>(2 * num_threads);

        let error_flag = Arc::new(AtomicBool::new(false));

        // Diff thread. This thread iterates the NodeDiffStreamer and passes the
        // items to the item processor thread.
        let error_flag_clone = error_flag.clone();
        let diff_thread = std::thread::spawn(move || {
            let diff_streamer = NodeDiffStreamer::new(previous_tree_streamer, fs_streamer);

            for diff_result in diff_streamer {
                if error_flag_clone.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                if let Ok(diff) = diff_result {
                    if let Err(e) = diff_tx.send(diff) {
                        error_flag_clone.fetch_and(true, std::sync::atomic::Ordering::AcqRel);
                        cli::log_error(&format!(
                            "Committer thread errored sending diff: {:?}",
                            e.to_string()
                        ));
                        break;
                    }
                } else {
                    cli::log_error("Committer thread errored getting next diff");
                    break;
                }
            }
        });

        // Item processor thread pool. These threads receive diffs and process them, chunking and
        // saving files in the process. The resulting processed nodes are passed to the serializer
        // thread.
        let diff_rx_clone = diff_rx.clone();
        let process_item_tx_clone = process_item_tx.clone();
        let repo_clone = repo.clone();
        let error_flag_clone = error_flag.clone();
        let processor_progress_reporter_clone = progress_reporter.clone();
        let commit_root_path_clone = commit_root_path.clone();
        let processor_thread = std::thread::spawn(move || {
            while let Ok(diff_tuple) = diff_rx_clone.recv() {
                if error_flag_clone.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                let inner_process_item_tx_clone = process_item_tx_clone.clone();
                let inner_repo_clone = repo_clone.clone();
                let inner_error_flag_clone = error_flag_clone.clone();
                let inner_progress_reporter_clone = processor_progress_reporter_clone.clone();
                let inner_commit_root_path_clone = commit_root_path_clone.clone();
                pool.spawn(move || {
                    // Notify reporter
                    if let Some(ref pr) = inner_progress_reporter_clone {
                        let (item_path, _, _, _) = &diff_tuple;
                        pr.processing_file(
                            item_path
                                .strip_prefix(inner_commit_root_path_clone.clone())
                                .unwrap()
                                .to_path_buf(),
                        );
                    }

                    let processed_item_result = Self::process_item(
                        diff_tuple,
                        inner_repo_clone.as_ref(),
                        inner_progress_reporter_clone,
                    );

                    match processed_item_result {
                        Ok(processed_item_opt) => {
                            if let Some(processed_item) = processed_item_opt {
                                if let Err(e) = inner_process_item_tx_clone.send(processed_item) {
                                    inner_error_flag_clone.store(true, Ordering::Release);
                                    cli::log_error(&format!(
                                        "Committer thread errored sending processing item: {:?}",
                                        e.to_string()
                                    ));
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            inner_error_flag_clone.store(true, Ordering::Release);
                            cli::log_error(&format!(
                                "Committer thread errored processing item: {:?}",
                                e.to_string()
                            ));
                            return;
                        }
                    }
                });
            }
        });

        // No one uses this copy of the tx (each worker thread just got a clone).
        // We must drop it or else the serializer thread will be blocked waiting for all copies
        // to be dropped.
        drop(process_item_tx);

        // Serializer thread. This thread receives processed items and serializes tree nodes as they
        // become finalized, bottom-up.
        let error_flag_clone = error_flag.clone();
        let repo_clone = repo.clone();
        let serializer_progress_reporter_clone = progress_reporter.clone();
        let serializer_commit_root_path_clone = commit_root_path.clone();
        let tree_serializer_thread = std::thread::spawn(move || {
            let mut final_root_tree_id: Option<ObjectId> = None;
            let mut pending_trees = Self::create_pending_trees(
                &serializer_commit_root_path_clone,
                &absolute_source_paths,
            );

            while let Ok(item) = process_item_rx.recv() {
                if error_flag_clone.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                // Notify reporter
                if let Some(ref pr) = serializer_progress_reporter_clone {
                    let (item_path, _) = &item;
                    pr.processed_file(
                        item_path
                            .clone()
                            .strip_prefix(serializer_commit_root_path_clone.clone())
                            .unwrap()
                            .to_path_buf(),
                    );
                }

                if let Err(e) = Self::handle_processed_item(
                    item,
                    repo_clone.as_ref(),
                    &mut pending_trees,
                    &mut final_root_tree_id,
                    &serializer_commit_root_path_clone,
                ) {
                    error_flag_clone.store(true, Ordering::Release);
                    cli::log_error(&format!(
                        "Committer thread errored handling processed item: {:?}",
                        e.to_string()
                    ));
                    break;
                }
            }

            let (_uncompressed, _compressed) = repo.flush()?;

            // The entire tree must be serialized by now, so we can create a
            // snapshot with the root tree id.
            match final_root_tree_id {
                Some(tree_id) => Ok(Snapshot {
                    timestamp: Local::now(),
                    tree: tree_id.clone(),
                    size: 0,
                    root: commit_root_path.clone(),
                    paths: absolute_source_paths.clone(),
                    description: None,
                }),
                None => Err(anyhow!("Failed to finalize snapshot")),
            }
        });

        // Join threads
        let _ = diff_thread.join();
        let _ = processor_thread.join();
        tree_serializer_thread.join().unwrap()
    }

    fn process_item(
        item: (PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff),
        repo: &dyn RepositoryBackend,
        progress_reporter: Option<Arc<CommitProgressReporter>>,
    ) -> Result<Option<(PathBuf, StreamNode)>> {
        let (path, prev_node, next_node, diff_type) = item;

        match diff_type {
            // Deleted item: We don't need to save anything and this node will not be present in the
            // serialized tree. We just ignore it. Maybe notify a progress reporter.
            NodeDiff::Deleted => {
                // Notify the reporter
                if let Some(pr) = progress_reporter {
                    match prev_node {
                        Some(node_info) => match node_info.node.node_type {
                            NodeType::File | NodeType::Symlink => {
                                pr.deleted_file();
                            }
                            NodeType::Directory => {
                                pr.deleted_dir();
                            }
                        },
                        None => bail!("Item deleted but the node was not provided"),
                    }
                }

                Ok(None)
            }

            // Unchanged item: No need to save the content, but we still need to serialize the node.
            NodeDiff::Unchanged => match prev_node {
                None => bail!("Item unchanged but the node was not provided"),
                Some(prev_stream_node_info) => {
                    let node = prev_stream_node_info.node.clone();
                    match node.node_type {
                        NodeType::File | NodeType::Symlink => {
                            // Notify reporter
                            if let Some(pr) = progress_reporter {
                                let bytes_processed = node.metadata.size;
                                pr.processed_bytes(bytes_processed);
                                pr.unchanged_file();
                            }

                            return Ok(Some((
                                path,
                                StreamNode {
                                    node,
                                    num_children: 0,
                                },
                            )));
                        }
                        NodeType::Directory => {
                            // Notify reporter
                            if let Some(pr) = progress_reporter {
                                pr.unchanged_dir();
                            }

                            return Ok(Some((path, prev_stream_node_info)));
                        }
                    }
                }
            },

            // New or changed item: We need to save the contents and serialize the node.
            NodeDiff::New | NodeDiff::Changed => match next_node {
                None => bail!("Item new or changed but the node was not provided"),
                Some(next_stream_node_info) => {
                    let mut node = next_stream_node_info.node.clone();
                    match node.node_type {
                        NodeType::File | NodeType::Symlink => {
                            if node.is_file() {
                                let (_, updated_node) =
                                    Archiver::save_file(repo, &path, progress_reporter.clone())
                                        .map(|chunk_result| {
                                            let mut updated_node = node.clone();
                                            updated_node.contents = Some(chunk_result);

                                            // Notify reporter
                                            if let Some(pr) = progress_reporter {
                                                if diff_type == NodeDiff::New {
                                                    pr.new_file();
                                                } else if diff_type == NodeDiff::Changed {
                                                    pr.changed_file();
                                                }
                                            }

                                            (path.to_path_buf(), updated_node.clone())
                                        })?;

                                node = updated_node;
                            }

                            return Ok(Some((
                                path,
                                StreamNode {
                                    node,
                                    num_children: 0,
                                },
                            )));
                        }

                        NodeType::Directory => {
                            // Notify reporter
                            if let Some(pr) = progress_reporter {
                                if diff_type == NodeDiff::New {
                                    pr.new_dir();
                                } else if diff_type == NodeDiff::Changed {
                                    pr.changed_dir();
                                }
                            }

                            return Ok(Some((path, next_stream_node_info)));
                        }
                    }
                }
            },
        }
    }

    fn handle_processed_item(
        processed_item: (PathBuf, StreamNode),
        repo: &dyn RepositoryBackend,
        pending_trees: &mut BTreeMap<PathBuf, PendingTree>,
        final_root_tree_id: &mut Option<ObjectId>,
        commit_root_path: &Path,
    ) -> Result<()> {
        let (path, stream_node) = processed_item;

        let mut dir_path = utils::extract_parent(&path).unwrap();

        match stream_node.node.node_type {
            NodeType::File | NodeType::Symlink => {
                Self::insert_finalized_node(pending_trees, &dir_path, stream_node.node);
            }
            NodeType::Directory => {
                let existing_pending_tree = pending_trees.insert(
                    path.clone(),
                    PendingTree {
                        node: Some(stream_node.node),
                        children: BTreeMap::new(),
                        num_expected_children: stream_node.num_children as isize,
                    },
                );

                match existing_pending_tree {
                    Some(old_pending_tree) => {
                        pending_trees.get_mut(&path).unwrap().children = old_pending_tree.children;
                    }
                    None => (),
                }

                dir_path = path;
            }
        }

        Self::finalize_if_complete(
            dir_path,
            repo,
            pending_trees,
            final_root_tree_id,
            commit_root_path,
        )
    }

    fn finalize_if_complete(
        dir_path: PathBuf,
        repo: &dyn RepositoryBackend,
        pending_trees: &mut BTreeMap<PathBuf, PendingTree>,
        final_root_tree_id: &mut Option<ObjectId>,
        commit_root_path: &Path,
    ) -> Result<()> {
        let this_pending_tree = match pending_trees.get(&dir_path) {
            Some(tree) => tree,
            None => {
                return Ok(());
            }
        };

        if this_pending_tree.is_pending() {
            return Ok(());
        }

        let this_pending_tree = pending_trees
            .remove(&dir_path)
            .with_context(|| "Completed tree not found in map during removal.")?;

        let completed_tree = Tree {
            nodes: this_pending_tree.children.into_values().collect(),
        };

        let tree_id_result: Result<ObjectId> = Archiver::save_tree(repo, &completed_tree);

        let tree_id = tree_id_result?;

        if dir_path == commit_root_path {
            *final_root_tree_id = Some(tree_id);
        } else {
            let mut completed_dir_node = this_pending_tree.node.with_context(|| {
                format!(
                    "Non-root finalized tree should have a node. dir_path: {}",
                    dir_path.display()
                )
            })?;
            completed_dir_node.tree = Some(tree_id);

            let parent_path = utils::extract_parent(&dir_path).unwrap_or_else(|| PathBuf::new());

            Self::insert_finalized_node(pending_trees, &parent_path, completed_dir_node.clone());

            let parent_pending_tree = pending_trees.get_mut(&parent_path).unwrap();
            let child_node_in_parent_list = parent_pending_tree.children.get_mut(&completed_dir_node.name)
                 .with_context(|| format!("Completed child node '{}' not found in parent's children map ('{}') during finalization propagation.", completed_dir_node.name, parent_path.display()))?;
            *child_node_in_parent_list = completed_dir_node;

            Self::finalize_if_complete(
                parent_path,
                repo,
                pending_trees,
                final_root_tree_id,
                commit_root_path,
            )?;
        }

        Ok(())
    }

    /// Puts a file into the repository
    ///
    /// This function will split the file into chunks for deduplication, which
    /// will be compressed, encrypted and stored in the repository.
    /// The content hash of each chunk is used to identify the chunk and determine
    /// if the chunk already exists in the repository.
    fn save_file(
        repo: &dyn RepositoryBackend,
        src_path: &Path,
        progress_reporter: Option<Arc<CommitProgressReporter>>,
    ) -> Result<Vec<ObjectId>> {
        let source = File::open(src_path)
            .with_context(|| format!("Could not open file \'{}\'", src_path.display()))?;
        let reader = BufReader::new(source);

        // The chunker parameters must remain stable across versions, otherwise
        // same contents will no longer produce same chunks and IDs.
        let chunker = StreamCDC::with_level(
            reader,
            backup::defaults::MIN_CHUNK_SIZE,
            backup::defaults::AVG_CHUNK_SIZE,
            backup::defaults::MAX_CHUNK_SIZE,
            Normalization::Level1,
        );

        let mut chunk_hashes = Vec::with_capacity(
            1 + (backup::defaults::MAX_PACK_SIZE / backup::defaults::AVG_CHUNK_SIZE as u64)
                as usize,
        );

        for result in chunker {
            let chunk = result?;
            let processed_size = chunk.data.len() as u64;

            let (raw_size, encoded_size, content_hash) =
                repo.save_blob(ObjectType::Data, chunk.data)?;

            chunk_hashes.push(content_hash);

            if let Some(ref pr) = progress_reporter {
                pr.encoded_bytes(encoded_size);
                pr.raw_bytes(raw_size);
                pr.processed_bytes(processed_size);
            }
        }

        Ok(chunk_hashes)
    }

    /// Saves a tree in the repository. This function should be called when a tree is complete,
    /// that is, when all the contents and/or tree hashes have been resolved.
    pub fn save_tree(repo: &dyn RepositoryBackend, tree: &Tree) -> Result<Hash> {
        let tree_json = serde_json::to_string(tree)?.as_bytes().to_vec();
        let (_raw_size, _encoded_size, hash) = repo.save_blob(ObjectType::Tree, tree_json)?;
        Ok(hash)
    }

    /// Load a tree from the repository.
    pub fn load_tree(repo: &dyn RepositoryBackend, root_id: &ObjectId) -> Result<Tree> {
        let tree_object = repo.load_blob(root_id)?;
        let tree: Tree = serde_json::from_slice(&tree_object)?;
        Ok(tree)
    }

    #[inline]
    fn insert_finalized_node(
        pending_trees: &mut BTreeMap<PathBuf, PendingTree>,
        parent_path: &Path,
        node: Node,
    ) {
        let parent_pending_tree = pending_trees
            .entry(parent_path.to_path_buf())
            .or_insert_with(|| PendingTree {
                node: None,
                children: BTreeMap::new(),
                num_expected_children: -1,
            });
        parent_pending_tree.children.insert(node.name.clone(), node);
    }

    fn create_pending_trees(
        commit_root_path: &Path,
        paths: &[PathBuf],
    ) -> BTreeMap<PathBuf, PendingTree> {
        let mut pending_trees = BTreeMap::new();

        // We need to know ahead how many children the root is expecting, because the FSNodeStreamer
        // does not emit it.
        let (root_children_count, _) = utils::intermediate_paths(commit_root_path, paths);

        // The tree root, has no node
        pending_trees.insert(
            commit_root_path.to_path_buf(),
            PendingTree {
                node: None,
                children: BTreeMap::new(),
                num_expected_children: root_children_count as isize,
            },
        );

        pending_trees
    }
}

#[cfg(test)]
mod test {}
