// mapache is an incremental backup tool
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
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::{
    global::ID,
    repository::{
        RepositoryBackend,
        streamers::StreamNode,
        tree::{Node, NodeType, Tree},
    },
    utils,
};

/// Represents the expected number of children for a directory node.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ExpectedChildren {
    /// The number of children is known.
    Known(usize),
    /// The number of children is not yet known (e.g., for the root before stream processing).
    Unknown,
}

impl From<isize> for ExpectedChildren {
    fn from(value: isize) -> Self {
        if value < 0 {
            ExpectedChildren::Unknown
        } else {
            ExpectedChildren::Known(value as usize)
        }
    }
}

/// Represents a directory node that is being built bottom-up during the snapshot process.
/// It holds the directory's own node information (if available), the collected child nodes,
/// and the number of children expected from the stream.
#[derive(Debug)]
pub(crate) struct PendingTree {
    pub num_expected_children: ExpectedChildren,
    pub node: Option<Node>,
    pub children: HashMap<String, Node>,
}

impl PendingTree {
    /// Returns true if this directory node is still waiting to receive children.
    fn is_pending(&self) -> bool {
        match self.num_expected_children {
            ExpectedChildren::Unknown => true,
            ExpectedChildren::Known(expected_count) => self.children.len() < expected_count,
        }
    }
}

pub(crate) fn init_pending_trees(
    snapshot_root_path: &Path,
    paths: &[PathBuf],
) -> HashMap<PathBuf, PendingTree> {
    let mut pending_trees = HashMap::new();

    // We need to know ahead how many children the root is expecting, because the FSNodeStreamer
    // does not emit it (the root node).
    let (root_children_count, _) = utils::get_intermediate_paths(snapshot_root_path, paths);

    // The tree root, has no node
    pending_trees.insert(
        snapshot_root_path.to_path_buf(),
        PendingTree {
            node: None,
            children: HashMap::new(),
            num_expected_children: ExpectedChildren::Known(root_children_count),
        },
    );

    pending_trees
}

pub(crate) fn handle_processed_item(
    (path, stream_node): (PathBuf, StreamNode),
    repo: &dyn RepositoryBackend,
    pending_trees: &mut HashMap<PathBuf, PendingTree>,
    final_root_tree_id: &mut Option<ID>,
    snapshot_root_path: &Path,
) -> Result<(u64, u64)> {
    let mut dir_path = utils::extract_parent(&path)
        .with_context(|| format!("Could not extract parent path for {}", path.display()))?;

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
            pending_trees
                .entry(path.clone())
                .and_modify(|pt| {
                    pt.node = Some(stream_node.node.clone());
                    pt.num_expected_children = ExpectedChildren::Known(stream_node.num_children);
                })
                .or_insert_with(|| PendingTree {
                    node: Some(stream_node.node),
                    children: HashMap::new(),
                    num_expected_children: ExpectedChildren::Known(stream_node.num_children),
                });

            dir_path = path;
        }
    }

    finalize_if_complete(
        dir_path,
        repo,
        pending_trees,
        final_root_tree_id,
        snapshot_root_path,
    )
}

pub(crate) fn finalize_if_complete(
    dir_path: PathBuf,
    repo: &dyn RepositoryBackend,
    pending_trees: &mut HashMap<PathBuf, PendingTree>,
    final_root_tree_id: &mut Option<ID>,
    snapshot_root_path: &Path,
) -> Result<(u64, u64)> {
    // Check if the tree exists and is not pending without consuming it
    let Some(this_pending_tree_peek) = pending_trees.get(&dir_path) else {
        return Ok((0, 0));
    };

    if this_pending_tree_peek.is_pending() {
        return Ok((0, 0));
    }

    // Now that we know it's complete, remove it
    let this_pending_tree = pending_trees.remove(&dir_path).with_context(|| {
        format!(
            "Completed tree for path '{}' not found in map during removal.",
            dir_path.display()
        )
    })?;

    let mut completed_tree = Tree {
        nodes: this_pending_tree.children.into_values().collect(),
    };

    let (tree_id, (raw_tree_size, encoded_tree_size)) = completed_tree.save_to_repo(repo)?;

    // If the current directory is the snapshot root, store its tree ID as the
    // final root ID. Otherwise, it's an intermediate directory, so update its
    // parent's children with this completed directory node.
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

        let parent_path = utils::extract_parent(&dir_path).with_context(|| {
            format!(
                "Could not extract parent path for finalized directory '{}'",
                dir_path.display()
            )
        })?;

        // Insert the completed directory node into its parent's children
        insert_finalized_node(pending_trees, &parent_path, completed_dir_node);

        // Recursively try to finalize the parent
        finalize_if_complete(
            parent_path,
            repo,
            pending_trees,
            final_root_tree_id,
            snapshot_root_path,
        )?;
    }

    Ok((raw_tree_size, encoded_tree_size))
}

#[inline]
fn insert_finalized_node(
    pending_trees: &mut HashMap<PathBuf, PendingTree>,
    parent_path: &Path,
    node: Node,
) {
    let parent_pending_tree = pending_trees
        .entry(parent_path.to_path_buf())
        .or_insert_with(|| PendingTree {
            node: None,
            children: HashMap::new(),
            // When a directory is inserted as a child, its parent's num_expected_children is still unknown.
            // This will be properly set when the parent directory itself is processed as a StreamNode.
            num_expected_children: ExpectedChildren::Unknown,
        });
    parent_pending_tree.children.insert(node.name.clone(), node);
}
