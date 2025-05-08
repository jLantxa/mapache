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
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
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
    pub num_expected_children: isize,
}

impl PendingTree {
    ///  Returns true if this directory node is still waiting to receive children
    pub fn is_pending(&self) -> bool {
        (self.num_expected_children >= 0)
            && (self.children.len() as isize) < self.num_expected_children
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

    pending_trees: Arc<Mutex<BTreeMap<PathBuf, PendingTree>>>,

    commit_root_path: PathBuf,
    absolute_source_paths: Vec<PathBuf>,

    final_root_tree_id: Arc<Mutex<Option<TreeId>>>,

    should_do_full_scan: bool,
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
        full_scan: bool,
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
        let committer = Arc::new(Committer {
            repo,
            pending_trees: Arc::new(Mutex::new(Self::create_pending_trees(
                &commit_root_path,
                &absolute_source_paths,
            ))),
            commit_root_path,
            absolute_source_paths,
            final_root_tree_id: Arc::new(Mutex::new(None)),
            should_do_full_scan: full_scan,
            dry_run,
        });

        let thread_pool = ThreadPool::new(workers);
        let error_signal = Arc::new(AtomicBool::new(false));
        for _ in 0..workers {
            let error_signal = error_signal.clone();
            let diff_clone = diff_streamer.clone();

            let pending_trees = committer.pending_trees.clone();
            let final_root_tree_id = committer.final_root_tree_id.clone();
            let repo = committer.repo.clone();
            let commit_root_path = committer.commit_root_path.clone();
            let dry_run = committer.dry_run;
            let full_scan = committer.should_do_full_scan;

            thread_pool.execute(move || {
                loop {
                    if error_signal.load(Ordering::SeqCst) {
                        break;
                    }

                    let diff_result = {
                        let mut diff = diff_clone.lock().unwrap();
                        diff.next()
                    };

                    match diff_result {
                        Some(Ok(item)) => {
                            #[cfg(debug_assertions)]
                            cli::log!("{:#?}", item.0.display());

                            let result = Self::process_item(
                                repo.clone(),
                                pending_trees.clone(),
                                final_root_tree_id.clone(),
                                &commit_root_path,
                                item,
                                dry_run,
                                full_scan,
                            );
                            if result.is_err() {
                                #[cfg(debug_assertions)]
                                cli::log!("{:#?}", result.unwrap_err());

                                error_signal.store(true, Ordering::Relaxed);
                                break;
                            }
                        }
                        Some(Err(err)) => {
                            #[cfg(debug_assertions)]
                            cli::log!("{:#?}", err);

                            error_signal.store(true, Ordering::Relaxed);
                            break;
                        }
                        None => break,
                    }
                }
            });
        }

        // Wait for all workers to finish
        thread_pool.join();

        if error_signal.load(Ordering::SeqCst) {
            bail!("Failed to commit");
        }

        // Return snapshot
        let final_root_tree_id = committer.final_root_tree_id.lock().unwrap().clone();
        match final_root_tree_id {
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
        repo: Arc<dyn RepositoryBackend>,
        pending_trees: Arc<Mutex<BTreeMap<PathBuf, PendingTree>>>,
        final_root_tree_id: Arc<Mutex<Option<TreeId>>>,
        commit_root_path: &Path,
        item: (PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff),
        dry_run: bool,
        should_do_full_scan: bool,
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

                    let node = prev_stream_node_info.node;
                    match node.node_type {
                        NodeType::File | NodeType::Symlink => {
                            if !dry_run && should_do_full_scan && node.is_file() {
                                let (_, updated_node) = repo
                                    .save_file(&path, dry_run)
                                    .map(|chunk_result| {
                                        let mut updated_node = node.clone();
                                        updated_node.contents = Some(chunk_result.chunks);
                                        (path.to_path_buf(), updated_node.clone())
                                    })
                                    .with_context(|| "Synchronous save_file failed")?;

                                // Mutex-guarded access to pending_trees
                                let mut pending_trees_guard = pending_trees.lock().unwrap();

                                Self::insert_finalized_node(
                                    &mut pending_trees_guard,
                                    &parent_path,
                                    updated_node,
                                );
                                // ==> Mutex dropped here <==
                            } else {
                                // Mutex-guarded access to pending_trees
                                let mut pending_trees_guard = pending_trees.lock().unwrap();

                                Self::insert_finalized_node(
                                    &mut pending_trees_guard,
                                    &parent_path,
                                    node,
                                );
                                // ==> Mutex dropped here <==
                            }

                            Self::finalize_if_complete(
                                parent_path,
                                repo,
                                pending_trees,
                                final_root_tree_id,
                                &commit_root_path,
                                dry_run,
                            )?;
                        }
                        NodeType::Directory => {
                            {
                                // Mutex-guarded access to pending_trees
                                let mut pending_trees_guard = pending_trees.lock().unwrap();
                                pending_trees_guard.insert(
                                    path.clone(),
                                    PendingTree {
                                        node: Some(node.clone()),
                                        children: BTreeMap::new(),
                                        num_expected_children: prev_stream_node_info.num_children
                                            as isize,
                                    },
                                );
                                // ==> Mutex dropped here <==
                            }

                            Self::finalize_if_complete(
                                path,
                                repo,
                                pending_trees,
                                final_root_tree_id,
                                commit_root_path,
                                dry_run,
                            )?;
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

                            // Non blocking file save
                            if !dry_run && node.is_file() {
                                let _ = repo
                                    .save_file(&path, dry_run)
                                    .map(|chunk_result| {
                                        let mut updated_node = node.clone();
                                        updated_node.contents = Some(chunk_result.chunks);
                                        (path.to_path_buf(), updated_node.clone())
                                    })
                                    .with_context(|| "Synchronous save_file failed")?;

                                // Mutex-guarded access to pending_trees
                                let mut pending_trees_guard = pending_trees.lock().unwrap();

                                Self::insert_finalized_node(
                                    &mut pending_trees_guard,
                                    &parent_path,
                                    node,
                                );
                                // ==> Mutex dropped here <==
                            } else {
                                // Mutex-guarded access to pending_trees
                                let mut pending_trees_guard = pending_trees.lock().unwrap();

                                Self::insert_finalized_node(
                                    &mut pending_trees_guard,
                                    &parent_path,
                                    node,
                                );
                                // ==> Mutex dropped here <==
                            }

                            Self::finalize_if_complete(
                                parent_path,
                                repo,
                                pending_trees,
                                final_root_tree_id,
                                commit_root_path,
                                dry_run,
                            )?;
                        }

                        NodeType::Directory => {
                            {
                                // Mutex-guarded access to pending_trees
                                let mut pending_trees_guard = pending_trees.lock().unwrap();

                                let existing_pending_tree = pending_trees_guard.insert(
                                    path.clone(),
                                    PendingTree {
                                        node: Some(node.clone()),
                                        children: BTreeMap::new(),
                                        num_expected_children: next_stream_node_info.num_children
                                            as isize,
                                    },
                                );

                                match existing_pending_tree {
                                    Some(old_pending_tree) => {
                                        pending_trees_guard.get_mut(&path).unwrap().children =
                                            old_pending_tree.children;
                                    }
                                    None => (),
                                }

                                // ==> Mutex dropped here <==
                            }

                            Self::finalize_if_complete(
                                path,
                                repo,
                                pending_trees,
                                final_root_tree_id,
                                commit_root_path,
                                dry_run,
                            )?;
                        }
                    }
                }
            },
        }

        Ok(())
    }

    fn finalize_if_complete(
        dir_path: PathBuf,
        repo: Arc<dyn RepositoryBackend>,
        pending_trees: Arc<Mutex<BTreeMap<PathBuf, PendingTree>>>,
        final_root_tree_id: Arc<Mutex<Option<TreeId>>>,
        commit_root_path: &Path,
        dry_run: bool,
    ) -> Result<()> {
        // Lock the mutex to modify the pending trees
        let mut pending_trees_guard = pending_trees.lock().unwrap();

        let this_pending_tree = match pending_trees_guard.get(&dir_path) {
            Some(tree) => tree,
            None => {
                return Ok(());
            }
        };

        if this_pending_tree.is_pending() {
            return Ok(());
        }

        let this_pending_tree = pending_trees_guard
            .remove(&dir_path)
            .context("Logic error: Completed tree not found in map during removal.")?;

        let completed_tree = Tree {
            nodes: this_pending_tree.children.into_values().collect(),
        };

        let tree_id: TreeId = if dry_run {
            TreeId::from("")
        } else {
            let tree_id_result: Result<TreeId> = repo
                .save_tree(&completed_tree, dry_run)
                .with_context(|| "Synchronous save_tree failed");

            tree_id_result?
        };

        if dir_path == commit_root_path {
            *final_root_tree_id.lock().unwrap() = Some(tree_id);
            // ==> Mutex dropped here <==
        } else {
            let mut completed_dir_node = this_pending_tree.node.with_context(|| {
                format!(
                    "Logic error: Non-root finalized tree should have a node. dir_path: {}",
                    dir_path.display()
                )
            })?;
            completed_dir_node.tree = Some(tree_id);

            let parent_path = Self::extract_parent(&dir_path).unwrap_or_else(|| PathBuf::new());

            Self::insert_finalized_node(
                &mut pending_trees_guard,
                &parent_path,
                completed_dir_node.clone(),
            );

            let parent_pending_tree = pending_trees_guard.get_mut(&parent_path).unwrap();
            let child_node_in_parent_list = parent_pending_tree.children.get_mut(&completed_dir_node.name)
                 .with_context(|| format!("Logic error: Completed child node '{}' not found in parent's children map ('{}') during finalization propagation.", completed_dir_node.name, parent_path.display()))?;
            *child_node_in_parent_list = completed_dir_node;

            drop(pending_trees_guard); // <== Mutex dropped here (before recursive call)

            Self::finalize_if_complete(
                parent_path,
                repo,
                pending_trees,
                final_root_tree_id,
                commit_root_path,
                dry_run,
            )?;
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

    #[inline]
    fn insert_finalized_node(
        pending_trees_guard: &mut MutexGuard<'_, BTreeMap<PathBuf, PendingTree>>,
        parent_path: &Path,
        node: Node,
    ) {
        match pending_trees_guard.get_mut(parent_path) {
            Some(parent_pending_tree) => {
                parent_pending_tree.children.insert(node.name.clone(), node);
            }
            None => {
                println!("Creating non-existent parent {}", parent_path.display());
                let _ = pending_trees_guard.insert(
                    parent_path.to_path_buf(),
                    PendingTree {
                        node: None,
                        children: BTreeMap::new(),
                        num_expected_children: isize::MAX,
                    },
                );
                pending_trees_guard
                    .get_mut(parent_path)
                    .unwrap()
                    .children
                    .insert(node.name.clone(), node);
            }
        }
    }
}
