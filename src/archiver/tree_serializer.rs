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
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};

use crate::{
    global::ID,
    repository::{
        RepositoryBackend,
        tree::{Node, NodeType, StreamNode, Tree},
    },
    ui::snapshot_progress::SnapshotProgressReporter,
    utils,
};

/// Represents a directory node that is being built bottom-up during the snapshot process.
/// It holds the directory's own node information (if available), the collected child nodes,
/// and the number of children expected from the stream.
#[derive(Debug)]
pub(crate) struct PendingTree {
    pub node: Option<Node>,
    pub children: BTreeMap<String, Node>,
    pub num_expected_children: isize,
}

impl PendingTree {
    ///  Returns true if this directory node is still waiting to receive children
    pub(crate) fn is_pending(&self) -> bool {
        self.num_expected_children < 0
            || (self.children.len() as isize) < self.num_expected_children
    }
}

pub(crate) fn init_pending_trees(
    snapshot_root_path: &Path,
    paths: &[PathBuf],
) -> BTreeMap<PathBuf, PendingTree> {
    let mut pending_trees = BTreeMap::new();

    // We need to know ahead how many children the root is expecting, because the FSNodeStreamer
    // does not emit it.
    let (root_children_count, _) = utils::intermediate_paths(snapshot_root_path, paths);

    // The tree root, has no node
    pending_trees.insert(
        snapshot_root_path.to_path_buf(),
        PendingTree {
            node: None,
            children: BTreeMap::new(),
            num_expected_children: root_children_count as isize,
        },
    );

    pending_trees
}

pub(crate) fn handle_processed_item(
    processed_item: (PathBuf, StreamNode),
    repo: &dyn RepositoryBackend,
    pending_trees: &mut BTreeMap<PathBuf, PendingTree>,
    final_root_tree_id: &mut Option<ID>,
    snapshot_root_path: &Path,
    progress_reporter: &Arc<SnapshotProgressReporter>,
) -> Result<()> {
    let (path, stream_node) = processed_item;

    let mut dir_path = utils::extract_parent(&path).unwrap();

    match stream_node.node.node_type {
        NodeType::File
        | NodeType::Symlink
        | NodeType::BlockDevice
        | NodeType::CharDevice
        | NodeType::Fifo
        | NodeType::Socket => {
            insert_finalized_node(pending_trees, &dir_path, stream_node.node);
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

    finalize_if_complete(
        dir_path,
        repo,
        pending_trees,
        final_root_tree_id,
        snapshot_root_path,
        progress_reporter,
    )
}

fn finalize_if_complete(
    dir_path: PathBuf,
    repo: &dyn RepositoryBackend,
    pending_trees: &mut BTreeMap<PathBuf, PendingTree>,
    final_root_tree_id: &mut Option<ID>,
    snapshot_root_path: &Path,
    progress_reporter: &Arc<SnapshotProgressReporter>,
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

    let (tree_id, raw_tree_size, encoded_tree_size) = completed_tree.save_to_repo(repo)?;

    // Notify reporter
    progress_reporter.written_meta_bytes(raw_tree_size, encoded_tree_size);

    if dir_path == snapshot_root_path {
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

        insert_finalized_node(pending_trees, &parent_path, completed_dir_node.clone());

        let parent_pending_tree = pending_trees.get_mut(&parent_path).unwrap();
        let child_node_in_parent_list = parent_pending_tree.children.get_mut(&completed_dir_node.name)
                 .with_context(|| format!("Completed child node '{}' not found in parent's children map ('{}') during finalization propagation.", completed_dir_node.name, parent_path.display()))?;
        *child_node_in_parent_list = completed_dir_node;

        finalize_if_complete(
            parent_path,
            repo,
            pending_trees,
            final_root_tree_id,
            snapshot_root_path,
            progress_reporter,
        )?;
    }

    Ok(())
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
