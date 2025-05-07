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
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use threadpool::ThreadPool;

use crate::{
    cli,
    repository::tree::{NodeType, Tree},
    utils,
};

use super::{
    backend::{RepositoryBackend, SnapshotId, TreeId},
    snapshot::Snapshot,
    tree::{FSNodeStreamer, Node, NodeDiff, NodeDiffStreamer, SerializedNodeStreamer, StreamNode},
};

/// Represents a directory node that is being built bottom-up during the commit process.
/// It holds the directory's own node information (if available), the collected child nodes,
/// and the number of children expected from the stream.
#[derive(Debug)]
struct PendingTree {
    pub node: Option<Node>,
    pub children: BTreeMap<String, Node>,
    pub num_expected_children: usize,
}

impl PendingTree {
    ///  Returns true if this directory node is still waiting to receive children
    pub fn is_pending(&self) -> bool {
        self.children.len() < self.num_expected_children
    }
}

/// Orchestrates the commit process, building a new snapshot of the source paths
/// by processing a diff stream against a previous snapshot.
///
/// This implementation uses a bottom-up approach with a shared, mutex-protected
/// map (`pending_trees`) to collect child nodes before serializing directory trees.
/// Worker threads consume diff items and update the shared state.
pub struct Committer {
    repo: Arc<dyn RepositoryBackend>,
    pending_trees: BTreeMap<PathBuf, PendingTree>,

    commit_root_path: PathBuf,
    absolute_source_paths: Vec<PathBuf>,

    final_root_tree_id: Option<TreeId>,

    dry_run: bool,
}

// TODO: Revisit this implementation and try to make it async to avoid blocking during file IO.
impl Committer {
    /// Runs the commiter and returns a Snapshot with the tree id of the serialized root tree.
    pub fn run(
        repo: Arc<dyn RepositoryBackend>,
        source_paths: &[PathBuf],
        parent_snapshot_id: Option<SnapshotId>,
        workers: usize,
        dry_run: bool,
    ) -> Result<Snapshot> {
        if workers < 1 {
            bail!("The number of committer workers must be at least 1");
        }

        // First convert the paths to absolute paths. canonicalize failes if the path does not exist.
        let mut absolute_source_paths = Vec::new();
        for path in source_paths {
            match std::fs::canonicalize(&path) {
                Ok(absolute_path) => absolute_source_paths.push(absolute_path),
                Err(e) => bail!(e),
            }
        }

        // Extract the commit root path
        let commit_root_path = if absolute_source_paths.is_empty() {
            cli::log_warning("No source paths provided. Creating empty commit.");
            PathBuf::new()
        } else if absolute_source_paths.len() == 1 {
            let single_source = absolute_source_paths.first().unwrap();
            if single_source == Path::new("/") {
                PathBuf::new()
            } else {
                single_source
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| PathBuf::new())
            }
        } else {
            utils::calculate_lcp(&absolute_source_paths)
        };

        // Extract parent snapshot tree id
        let parent_tree_id: Option<TreeId> = match parent_snapshot_id {
            None => None,
            Some(snapshot_id) => match repo.load_snapshot(&snapshot_id) {
                Ok(snap_option) => match snap_option {
                    None => None,
                    Some(s) => Some(s.tree),
                },
                Err(_) => bail!("Failed to load snapshot with id \'{}\'", snapshot_id),
            },
        };

        // Create streamers
        let fs_streamer = match FSNodeStreamer::from_paths(&absolute_source_paths) {
            Ok(stream) => stream,
            Err(_) => bail!("Failed to create FSNodeStreamer"),
        };
        let previous_tree_streamer = SerializedNodeStreamer::new(repo.clone(), parent_tree_id);
        let diff_streamer = Arc::new(Mutex::new(NodeDiffStreamer::new(
            previous_tree_streamer,
            fs_streamer,
        )));

        // Create the initial pending trees with the snapshot root and all intermediate parents
        let pending_trees = Self::create_pending_trees(&commit_root_path, &absolute_source_paths);

        let committer = Arc::new(Mutex::new(Committer {
            repo,
            pending_trees,
            commit_root_path,
            absolute_source_paths,
            final_root_tree_id: None,
            dry_run,
        }));

        let thread_pool = ThreadPool::new(workers);
        let error_signal = Arc::new(AtomicBool::new(false));
        for _ in 0..workers {
            let error_signal = error_signal.clone();
            let commiter_clone = committer.clone();
            let diff_clone = diff_streamer.clone();

            thread_pool.execute(move || {
                // Consume diffs and process them until the iterator is consumed
                loop {
                    if true == error_signal.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }

                    let mut diff_streamer = diff_clone.lock().unwrap();
                    let mut committer = commiter_clone.lock().unwrap();

                    match diff_streamer.next() {
                        Some(diff_result) => match diff_result {
                            Ok(item) => {
                                if let Err(_) = committer.process_item(item) {
                                    error_signal
                                        .fetch_and(true, std::sync::atomic::Ordering::Relaxed);
                                    break;
                                }
                            }
                            Err(_) => {
                                error_signal.fetch_and(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }
                        },
                        None => break,
                    }
                }
            });
        }

        // Wait for all workers to finish
        thread_pool.join();

        if error_signal.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("Processing error");
        }

        // Return snapshot
        let committer = committer.lock().unwrap();
        match &committer.final_root_tree_id {
            Some(tree_id) => Ok(Snapshot {
                timestamp: Utc::now(),
                tree: tree_id.clone(),
                paths: committer.absolute_source_paths.clone(),
                description: None,
            }),
            None => Err(anyhow!("Failed to finalize snapshot tree")),
        }
    }

    fn process_item(
        &mut self,
        item: (PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff),
    ) -> Result<()> {
        let (path, prev_node, next_node, diff_type) = item;

        match diff_type {
            // Deleted item: We don't need to save anything and this node will not be present in the
            // serialized tree. We just ignore it.
            NodeDiff::Deleted => (),

            // Unchanged item: No need to save the content, but we still need to serialize the node.
            NodeDiff::Unchanged => match prev_node {
                None => bail!("Item unchanged but the node was not provided"),
                Some(prev_stream_node_info) => {
                    let parent_path = Self::extract_parent(&path).unwrap_or_else(|| PathBuf::new());
                    let parent_pending_tree =
                        self.pending_trees.get_mut(&parent_path).ok_or_else(|| {
                            anyhow::anyhow!(
                            "Logic error: Parent path '{}' not found in pending trees for item '{}'.
                                Maybe streamer order is wrong or a parent was deleted/errored?",
                            parent_path.display(),
                            path.display()
                        )
                        })?;

                    let node = prev_stream_node_info.node;
                    match node.node_type {
                        NodeType::File | NodeType::Symlink => {
                            parent_pending_tree.children.insert(node.name.clone(), node);
                            self.finalize_if_complete(parent_path)?;
                        }
                        NodeType::Directory => {
                            self.pending_trees.insert(
                                path.clone(),
                                PendingTree {
                                    node: Some(node.clone()),
                                    children: BTreeMap::new(),
                                    num_expected_children: prev_stream_node_info.num_children,
                                },
                            );

                            self.finalize_if_complete(path)?;
                        }
                    }
                }
            },

            // New or changed item: We need to save the contents and serialize the node.
            NodeDiff::New | NodeDiff::Changed => match next_node {
                None => bail!("Item new or changed but the node was not provided"),
                Some(next_stream_node_info) => {
                    let node = next_stream_node_info.node;
                    match node.node_type {
                        NodeType::File | NodeType::Symlink => {
                            let parent_path =
                                Self::extract_parent(&path).unwrap_or_else(|| PathBuf::new());
                            let parent_pending_tree = self.pending_trees.get_mut(&parent_path).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "Logic error: Parent path '{}' not found in pending trees for item '{}'.
                                        Maybe streamer order is wrong or a parent was deleted/errored?",
                                    parent_path.display(),
                                    path.display()
                                )
                            })?;

                            if !self.dry_run && node.is_file() {
                                self.repo
                                    .save_file(&path)
                                    .map(|chunk_result| {
                                        let mut updated_node = node.clone();
                                        updated_node.contents = Some(chunk_result.chunks);
                                        (path, updated_node)
                                    })
                                    .with_context(|| "Synchronous save_file failed")?;
                            }

                            parent_pending_tree.children.insert(node.name.clone(), node);
                            self.finalize_if_complete(parent_path)?;
                        }

                        NodeType::Directory => {
                            // Directories have no content to save, just a node to serialize
                            if self.pending_trees.contains_key(&path) {
                                bail!(
                                    "Logic error: Directory path '{}' already exists in pending_trees map.",
                                    path.display()
                                );
                            }

                            self.pending_trees.insert(
                                path.clone(),
                                PendingTree {
                                    node: Some(node.clone()),
                                    children: BTreeMap::new(),
                                    num_expected_children: next_stream_node_info.num_children,
                                },
                            );

                            self.finalize_if_complete(path)?;
                        }
                    }
                }
            },
        }

        Ok(())
    }

    fn finalize_if_complete(&mut self, dir_path: PathBuf) -> Result<()> {
        let this_pending_tree = match self.pending_trees.get(&dir_path) {
            Some(tree) => tree,
            None => {
                // TODO: What happens if there is no pending tree now?
                // I could insert the new pending tree and later just update it when
                // another task tries to add it. This might happens if we process a file
                // before its parent directory. We would add the file to the new pending tree
                // and later just merge.
                return Ok(());
            }
        };

        if this_pending_tree.is_pending() {
            return Ok(());
        }

        let this_pending_tree = self
            .pending_trees
            .remove(&dir_path)
            .context("Logic error: Completed tree not found in map during removal.")?;

        let completed_tree = Tree {
            nodes: this_pending_tree.children.into_values().collect(),
        };

        let tree_id: TreeId = if self.dry_run {
            TreeId::from("")
        } else {
            let tree_id_result: Result<TreeId> = self
                .repo
                .save_tree(&completed_tree)
                .with_context(|| "Synchronous save_tree failed");

            tree_id_result?
        };

        if dir_path == self.commit_root_path {
            self.final_root_tree_id = Some(tree_id);
        } else {
            let mut completed_dir_node = this_pending_tree
                .node
                .with_context(|| "Logic error: Non-root finalized tree should have a node.")?;

            completed_dir_node.tree = Some(tree_id);

            let parent_path = Self::extract_parent(&dir_path).unwrap_or_else(|| PathBuf::new());

            let parent_pending_tree = self.pending_trees.get_mut(&parent_path)
                .with_context(|| format!("Logic error: Parent path '{}' not found during finalization propagation for child '{}'.", parent_path.display(), dir_path.display()))?;
            parent_pending_tree
                .children
                .insert(completed_dir_node.name.clone(), completed_dir_node.clone());

            let child_node_in_parent_list = parent_pending_tree.children.get_mut(&completed_dir_node.name)
                 .with_context(|| format!("Logic error: Completed child node '{}' not found in parent's children map ('{}') during finalization propagation.", completed_dir_node.name, parent_path.display()))?;

            *child_node_in_parent_list = completed_dir_node;

            self.finalize_if_complete(parent_path)?;
        }

        Ok(())
    }

    fn extract_parent(path: &Path) -> Option<PathBuf> {
        path.parent().map(|p| p.to_path_buf())
    }

    fn create_pending_trees(
        commit_root_path: &Path,
        absolute_source_paths: &[PathBuf],
    ) -> BTreeMap<PathBuf, PendingTree> {
        let mut pending_trees = BTreeMap::new();

        // The tree root, has no node
        pending_trees.insert(
            commit_root_path.to_path_buf(),
            PendingTree {
                node: None,
                children: BTreeMap::new(),
                num_expected_children: 0,
            },
        );

        let mut intermediate_paths = BTreeSet::<PathBuf>::new();

        for path in absolute_source_paths {
            let mut root_path = Self::extract_parent(&path).unwrap_or_else(|| PathBuf::new());

            while root_path.cmp(&commit_root_path.to_path_buf()) == std::cmp::Ordering::Greater {
                intermediate_paths.insert(root_path.clone());
                root_path = Self::extract_parent(&root_path).unwrap_or_else(|| PathBuf::new());
            }
        }

        for path in intermediate_paths {
            pending_trees.insert(
                path.clone(),
                PendingTree {
                    node: Some(Node::from_path(path.clone()).unwrap()),
                    children: BTreeMap::new(),
                    num_expected_children: 0,
                },
            );

            let parent = Self::extract_parent(&path).unwrap_or_else(|| PathBuf::new());
            pending_trees
                .get_mut(&parent)
                .unwrap()
                .num_expected_children += 1;
        }

        for path in absolute_source_paths {
            let parent_path = Self::extract_parent(&path).unwrap_or_else(|| PathBuf::new());
            pending_trees
                .get_mut(&parent_path)
                .unwrap()
                .num_expected_children += 1;
        }

        pending_trees
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use crate::{
        repository::{self},
        storage_backend::localfs::LocalFS,
    };

    use super::*;

    // This test is only used for debugging the Committer and will be deleted soon.
    // It must be replaced with a test that verifies that objects are correctly stored and
    // can be retrieved from the repository.
    #[test]
    #[ignore = "For debugging only"]
    fn test_commit() -> Result<()> {
        let temp_dir = tempdir()?;
        let repo_path = temp_dir.path().join("repo");

        let storage_backend = Arc::new(LocalFS::new());

        let repo: Arc<dyn RepositoryBackend> = Arc::from(repository::backend::init(
            storage_backend,
            &repo_path,
            String::from("mapachito"),
        )?);

        let paths = vec![PathBuf::from("./src"), PathBuf::from("./testdata")];

        let snapshot_result = Committer::run(repo.clone(), &paths, None, 2, false)?;

        println!("{:?}", snapshot_result);

        let _tree_streamer =
            SerializedNodeStreamer::new(Arc::from(repo), Some(snapshot_result.tree));

        Ok(())
    }
}
