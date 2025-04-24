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
    collections::{BTreeMap, HashMap},
    fs::ReadDir,
    path::Path,
};

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use super::metadata::Metadata;
use crate::utils::Hash;

pub type NodeIndex = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Node {
    File(FileNode),
    Directory(DirectoryNode),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryNode {
    pub name: String,
    #[serde(flatten)]
    pub metadata: Option<Metadata>,
    pub children: BTreeMap<String, NodeIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileNode {
    pub name: String,
    #[serde(flatten)]
    pub metadata: Option<Metadata>,
    pub chunks: Vec<Hash>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableTreeObject {
    pub name: String,
    #[serde(flatten)]
    pub metadata: Option<Metadata>,
    pub files: Vec<FileNode>,
    pub directories: BTreeMap<String, Hash>,
}

#[derive(Debug, Default)]
pub struct ScanResult {
    pub total_file_size: usize,
    pub num_files: usize,
    pub num_dirs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tree {
    nodes: Vec<Node>,

    #[serde(skip)]
    hashes: HashMap<NodeIndex, Hash>,
}

impl Node {
    /// Creates a new file node
    pub fn new_file(name: String, metadata: Option<Metadata>) -> Self {
        Node::File(FileNode {
            name,
            metadata,
            chunks: Vec::new(),
        })
    }

    /// Creates a new directory node
    pub fn new_dir(name: String, metadata: Option<Metadata>) -> Self {
        Node::Directory(DirectoryNode {
            name,
            metadata,
            children: BTreeMap::new(),
        })
    }

    /// Returns true if this node is a file
    pub fn is_file(&self) -> bool {
        if let Node::File { .. } = self {
            return true;
        }

        false
    }

    /// Returns true if this node is a directory
    pub fn is_directory(&self) -> bool {
        if let Node::Directory { .. } = self {
            return true;
        }

        false
    }
}

impl ScanResult {
    /// Merges this `ScanResult` with another one by accumulating the values
    pub fn merge(&mut self, other: &ScanResult) {
        self.total_file_size += other.total_file_size;
        self.num_dirs += other.num_dirs;
        self.num_files += other.num_files;
    }
}

impl Tree {
    /// Creates a new `Tree` with a root node
    pub fn new_with_root(node: Node) -> Self {
        let mut arena = Tree {
            nodes: Vec::new(),
            hashes: HashMap::new(),
        };
        arena.nodes.push(node);
        arena
    }

    /// Allocates a new node into the arena with no relation to any other nodes
    fn add_node(&mut self, node: Node) -> NodeIndex {
        let new_index = self.nodes.len();
        self.nodes.push(node);
        new_index
    }

    /// Returns an inmutable reference to the node with the given index.
    pub fn get(&self, index: NodeIndex) -> Option<&Node> {
        self.nodes.get(index)
    }

    /// Returns a mutable reference to the node with the given index
    pub fn get_mut(&mut self, index: NodeIndex) -> Option<&mut Node> {
        self.nodes.get_mut(index)
    }

    /// Adds a new child node to the tree, using the node at `parent_index` as the parent
    ///
    /// This function returns an error if:
    /// a. The parent index is invalid
    /// b. A file node is used as a parent (only directories can have children nodes)
    pub fn add_child(&mut self, child_node: Node, parent_index: NodeIndex) -> Result<NodeIndex> {
        if parent_index >= self.nodes.len() {
            bail!(format!("Invalid parent index \'{}\'", parent_index));
        } else if let Some(Node::File { .. }) = self.get(parent_index) {
            // Check that the parent is not a file
            bail!("Cannot add a child node to a file");
        }

        let child_name = match &child_node {
            Node::File(file_node) => file_node.name.clone(),
            Node::Directory(directory_node) => directory_node.name.clone(),
        };

        let child_index = self.add_node(child_node);

        // At this point, the parent can only be a directory
        if let Node::Directory(directory_node) = self.get_mut(parent_index).unwrap() {
            directory_node
                .children
                .insert(child_name.to_string(), child_index);
        }

        Ok(child_index)
    }

    /// Add a tree as a child of a node in this tree.
    ///
    /// The subtree is moved into this tree. If you want to preserve a copy of the subtree,
    /// pass a clone.
    ///
    /// As usual, only directories can be used as parent nodes/
    pub fn add_tree(&mut self, tree: Tree, parent_index: NodeIndex) -> Result<NodeIndex> {
        if parent_index > self.nodes.len() {
            bail!(format!("Invalid parent index \'{}\'", parent_index));
        }

        let sub_tree_index = self.nodes.len();
        let child_name = tree.get(0).unwrap().name();

        // Append the subtree to the arena
        match self.get_mut(parent_index).expect("Expected a node") {
            Node::File(_) => {
                // Check that the parent is not a file
                bail!("Cannot add a child node to a file");
            }
            Node::Directory(directory_node) => {
                directory_node
                    .children
                    .insert(child_name.to_owned(), sub_tree_index);
                self.nodes.extend(tree.nodes);
            }
        }

        // Now we need to update the child indices of the new nodes. Since we just appended
        // the nodes, we only need to offset all indices by the subtree index. We do this for
        // directory nodes.
        for node in &mut self.nodes[sub_tree_index..] {
            if let Node::Directory(directory_node) = node {
                for (_, index) in &mut directory_node.children {
                    *index += sub_tree_index;
                }
            }
        }

        Ok(sub_tree_index)
    }

    /// Returns the hash of the node with the given index.
    ///
    /// This function returns None if the hash does not exist.
    /// A hash may not exist due to:
    /// 1. The node with the given index does not exist
    /// 2. The hashes have not been calculated (see `refresh_hashes`)
    pub fn get_hash(&self, index: NodeIndex) -> Option<&Hash> {
        self.hashes.get(&index)
    }

    /// Updates the hashes of every node in the Tree arena
    ///
    /// This function must be called before serializing a `Tree` using the
    /// `SerializableTreeObject` format or before reading any hash value from the
    /// tree.
    ///
    /// The function uses the DFS postorder iterator to calculate the hashes,
    /// since the hash of a node depends directly on the hashes of its children.
    /// This is done to avoid a recursive traversal, which can cause stack overflows
    /// with very deep trees.
    pub fn refresh_hashes(&mut self) -> Result<()> {
        self.hashes.clear();
        let postorder_indices: Vec<NodeIndex> = self.iter_postorder().collect();

        for node_index in postorder_indices {
            let node = self
                .get(node_index)
                .ok_or_else(|| anyhow!("Failed to get node at index {}", node_index))?;

            let mut hasher = blake3::Hasher::new();

            hasher.update(node.name().as_bytes());

            if let Some(metadata) = node.metadata() {
                let metadata_str = serde_json::to_string(&metadata)?;
                let metadata_bytes = metadata_str.as_bytes();
                hasher.update(&metadata_bytes);
            }

            match node {
                Node::File(file_node) => {
                    for chunk_hash in &file_node.chunks {
                        hasher.update(chunk_hash.as_bytes());
                    }
                }
                Node::Directory(directory_node) => {
                    for (child_name, child_index) in &directory_node.children {
                        let child_hash = self.hashes.get(child_index).ok_or_else(|| {
                            anyhow!(
                                "Child hash not found for index {} (child of {})",
                                child_index,
                                node_index
                            )
                        })?;
                        hasher.update(child_name.as_bytes());
                        hasher.update(child_hash.as_bytes());
                    }
                }
            }

            let final_hash = hasher.finalize().to_string();
            self.hashes.insert(node_index, final_hash);
        }

        Ok(())
    }

    /// Produces a `SerializableTreeObject` for the node with the given index.
    ///
    /// The `SerializableTreeObject` is a serializable form of a directory node that references
    /// children directories by hash. This format is intended for the storage of Tree objects
    /// using content hashes for identification of directory nodes.
    ///
    /// This function requires that the `Tree` hashes be updated.
    pub fn to_serializable_object(&self, index: NodeIndex) -> Result<SerializableTreeObject> {
        let node = self
            .get(index)
            .ok_or_else(|| anyhow!("Failed to get node at index {}", index))?;

        match node {
            Node::File(_) => bail!("Cannot serialize a file node as a tree object"),
            Node::Directory(directory_node) => {
                let mut serializable_children: BTreeMap<String, Hash> = BTreeMap::new();
                let mut serializable_files: Vec<FileNode> = Vec::new();

                for (child_name, child_index) in &directory_node.children {
                    let child_node = self
                        .get(*child_index)
                        .expect(&format!("Expected a node with index \'{}\'", child_index));

                    match child_node {
                        Node::File(file_node) => serializable_files.push(file_node.clone()),
                        Node::Directory { .. } => {
                            let child_hash = self.hashes.get(&child_index).ok_or_else(|| {
                                anyhow!(
                                    "Child hash not found for index {} (child of {})",
                                    child_index,
                                    index
                                )
                            })?;

                            serializable_children.insert(child_name.clone(), child_hash.clone());
                        }
                    }
                }
                Ok(SerializableTreeObject {
                    name: directory_node.name.clone(),
                    metadata: directory_node.metadata.clone(),
                    directories: serializable_children,
                    files: serializable_files,
                })
            }
        }
    }

    /// Creates a pre-order iterator for the tree arena.
    /// Yields NodeIndex in pre-order (parent before children).
    pub fn iter_preorder(&self) -> TreePreorderIterator {
        let mut stack = Vec::new();
        if !self.nodes.is_empty() {
            // Start with the root node
            stack.push(0);
        }
        TreePreorderIterator { arena: self, stack }
    }

    /// Creates a post-order iterator for the tree arena.
    /// Yields NodeIndex in post-order (children before parent).
    pub fn iter_postorder(&self) -> TreePostorderIterator {
        let mut stack = Vec::new();
        if !self.nodes.is_empty() {
            // Start with the root node, marked as 'entering'
            stack.push((0, true)); // (NodeIndex, EnteringState)
        }
        TreePostorderIterator { arena: self, stack }
    }

    fn node_from_path(path: &Path) -> Result<(Node, ScanResult)> {
        let metadata = match std::fs::metadata(path) {
            Ok(meta) => Some(Metadata {
                size: meta.len(),
                modified: meta.modified().ok(),
                created: meta.created().ok(),

                // TODO:
                permissions: None,
                owner_uid: None,
                owner_gid: None,
            }),
            Err(_) => None,
        };

        if path.is_file() {
            let scan_result = ScanResult {
                total_file_size: metadata.as_ref().map(|meta| meta.size).unwrap_or(0) as usize,
                num_files: 1,
                num_dirs: 0,
            };
            Ok((
                Node::new_file(
                    path.file_name().unwrap().to_str().unwrap().to_owned(),
                    metadata,
                ),
                scan_result,
            ))
        } else {
            let scan_result = ScanResult {
                total_file_size: 0,
                num_files: 0,
                num_dirs: 1,
            };

            Ok((
                Node::new_dir(
                    path.file_name().unwrap().to_str().unwrap().to_owned(),
                    metadata,
                ),
                scan_result,
            ))
        }
    }

    /// Create a tree mimicking the structure of a file tree using a path.
    /// This function will scan all files and directories in the path recursively.
    pub fn from_path(path: &Path) -> Result<(Self, ScanResult)> {
        let mut scan_result = ScanResult::default();

        let (root_node, subscan_result) = Self::node_from_path(path)?;
        let mut tree = Tree::new_with_root(root_node);
        scan_result.merge(&subscan_result);

        let mut stack: Vec<(std::io::Result<ReadDir>, NodeIndex)> = Vec::new();
        stack.push((path.read_dir(), 0));

        while let Some((Ok(read_dir), parent_index)) = stack.pop() {
            for entry in read_dir {
                let path = entry?.path();

                if path.is_file() {
                    let (file_node, subscan_result) = Self::node_from_path(&path)?;
                    let _ = tree.add_child(file_node, parent_index);
                    scan_result.merge(&subscan_result);
                } else {
                    let (dir_node, subscan_result) = Self::node_from_path(&path)?;
                    let dir_index = tree.add_child(dir_node, parent_index)?;
                    scan_result.merge(&subscan_result);
                    stack.push((path.read_dir(), dir_index));
                }
            }
        }

        Ok((tree, scan_result))
    }
}

// Helper methods for Node enum
impl Node {
    pub fn name(&self) -> &str {
        match self {
            Node::File(file_node) => file_node.name.as_ref(),
            Node::Directory(directory_node) => directory_node.name.as_ref(),
        }
    }

    pub fn metadata(&self) -> &Option<Metadata> {
        match self {
            Node::File(file_node) => &file_node.metadata,
            Node::Directory(directory_node) => &directory_node.metadata,
        }
    }
}

pub struct TreePostorderIterator<'a> {
    arena: &'a Tree,
    stack: Vec<(NodeIndex, bool)>,
}

impl<'a> Iterator for TreePostorderIterator<'a> {
    type Item = NodeIndex;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((node_index, entering)) = self.stack.pop() {
            if entering {
                let node = self
                    .arena
                    .get(node_index)
                    .expect("Iterator state error: Invalid node index on stack");

                self.stack.push((node_index, false));

                if let Node::Directory(directory_node) = node {
                    for (_child_name, child_index) in directory_node.children.iter().rev() {
                        self.stack.push((*child_index, true));
                    }
                }
            } else {
                return Some(node_index);
            }
        }
        None
    }
}
/// A DFS preorder iterator for the `Tree`
pub struct TreePreorderIterator<'a> {
    arena: &'a Tree,
    stack: Vec<NodeIndex>,
}

impl<'a> Iterator for TreePreorderIterator<'a> {
    type Item = NodeIndex;

    fn next(&mut self) -> Option<Self::Item> {
        let node_index = self.stack.pop()?;

        let node = self
            .arena
            .get(node_index)
            .expect("Iterator state error: Invalid node index on stack");

        if let Node::Directory(directory_node) = node {
            for (_child_name, child_index) in directory_node.children.iter().rev() {
                self.stack.push(*child_index);
            }
        }

        Some(node_index)
    }
}

#[cfg(test)]
mod test {
    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use std::time::SystemTime;

    use crate::{testing, utils};

    use super::*;

    fn system_time(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> SystemTime {
        DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(year, month, day)
                .unwrap()
                .and_time(NaiveTime::from_hms_opt(hour, min, sec).unwrap()),
            Utc,
        )
        .into()
    }

    #[test]
    fn test_allocation() -> Result<()> {
        // This tree is used to construct testdata/tree0.json

        let mut tree = Tree::new_with_root(Node::new_dir(
            "dir0".to_string(),
            Some(Metadata {
                size: 13,
                modified: Some(system_time(2025, 04, 23, 18, 13, 00)),
                created: Some(system_time(2025, 04, 23, 17, 13, 00)),
                permissions: Some(0x777),
                owner_uid: Some(1234),
                owner_gid: Some(5678),
            }),
        ));
        let _ = tree.add_child(
            Node::File(FileNode {
                name: "file0".to_string(),
                metadata: Some(Metadata {
                    size: 33,
                    modified: Some(system_time(2025, 04, 23, 18, 14, 00)),
                    created: Some(system_time(2025, 04, 23, 17, 14, 00)),
                    permissions: Some(0x777),
                    owner_uid: Some(1234),
                    owner_gid: Some(5678),
                }),
                chunks: vec!["abcd".to_string(), "1234".to_string()],
            }),
            0,
        )?;
        let dir1 = tree.add_child(
            Node::new_dir(
                "dir1".to_string(),
                Some(Metadata {
                    size: 33,
                    modified: Some(system_time(2025, 04, 23, 18, 15, 00)),
                    created: Some(system_time(2025, 04, 23, 17, 15, 00)),
                    permissions: Some(0x777),
                    owner_uid: Some(1234),
                    owner_gid: Some(5678),
                }),
            ),
            0,
        )?;
        let _ = tree.add_child(
            Node::File(FileNode {
                name: "file1".to_string(),
                metadata: Some(Metadata {
                    size: 33,
                    modified: Some(system_time(2025, 04, 23, 18, 16, 00)),
                    created: Some(system_time(2025, 04, 23, 17, 16, 00)),
                    permissions: Some(0x777),
                    owner_uid: Some(1234),
                    owner_gid: Some(5678),
                }),
                chunks: vec!["aabb".to_string(), "1122".to_string()],
            }),
            dir1,
        )?;
        let _ = tree.add_child(
            Node::File(FileNode {
                name: "file2".to_string(),
                metadata: Some(Metadata {
                    size: 33,
                    modified: Some(system_time(2025, 04, 23, 18, 17, 00)),
                    created: Some(system_time(2025, 04, 23, 17, 17, 00)),
                    permissions: Some(0x777),
                    owner_uid: Some(1234),
                    owner_gid: Some(5678),
                }),
                chunks: vec!["00aabb".to_string(), "aa1122".to_string()],
            }),
            dir1,
        )?;

        assert_eq!(tree.get(0).expect("Expected a root node").name(), "dir0");
        assert_eq!(tree.get(1).expect("Expected a node").name(), "file0");
        assert_eq!(tree.get(2).expect("Expected a node").name(), "dir1");
        assert_eq!(tree.get(3).expect("Expected a node").name(), "file1");
        assert_eq!(tree.get(4).expect("Expected a node").name(), "file2");

        Ok(())
    }

    #[test]
    fn serialized_tree_object() -> Result<()> {
        let tree0_path = testing::get_test_path("tree0.json");
        let mut tree: Tree = utils::load_json(&tree0_path)?;

        tree.refresh_hashes()?;
        assert_eq!(
            tree.get_hash(0).expect("Expected a hash"),
            "cf4e03d18518c555ad8704fe4598b99c9f3122604dcbba6acebcaef2b2b43c79"
        );
        assert_eq!(
            tree.get_hash(1).expect("Expected a hash"),
            "53f07fdb4c883fe2173f1c758819779d61e06f7cefd2f977f102685ec5e7cde2"
        );
        assert_eq!(
            tree.get_hash(2).expect("Expected a hash"),
            "02681a9f30c5ff9b017b6afcae3b8b95a6036dddcf1ca6a2d039442bc327979a"
        );
        assert_eq!(
            tree.get_hash(3).expect("Expected a hash"),
            "af7a826b391786e1bc79794462257cab39e117db281edac6b3fd4b49f140452d"
        );
        assert_eq!(
            tree.get_hash(4).expect("Expected a hash"),
            "53058d88b96fcffa4a0b81531a1add5fe4cd29f780d6fb880f4109e78e86e035"
        );

        for node_index in tree.iter_preorder() {
            let node = tree
                .get(node_index)
                .expect(&format!("Expected a node with index \'{}\'", node_index));

            match node {
                Node::File(_) => continue,
                Node::Directory(directory_node) => {
                    let serialized_node = tree.to_serializable_object(node_index)?;

                    assert_eq!(directory_node.name, serialized_node.name);
                    assert_eq!(directory_node.metadata, serialized_node.metadata);
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_add_subtree() -> Result<()> {
        let tree0_path = testing::get_test_path("tree0.json");
        let mut tree: Tree = utils::load_json(&tree0_path)?;

        assert!(tree.get(5).is_none());
        if let Node::Directory(directory_node) = tree.get(0).expect("Expected root node") {
            assert_eq!(directory_node.children.len(), 2);
            assert!(directory_node.children.get("subtree").is_none());
        }

        let subtree = Tree::new_with_root(Node::new_dir("subtree".to_string(), None));
        tree.add_tree(subtree, 0)?; // Append subtree at the root

        assert_eq!(tree.get(5).expect("Expected a node").name(), "subtree");
        if let Node::Directory(directory_node) = tree.get(0).expect("Expected root node") {
            assert_eq!(directory_node.children.len(), 3);
            assert_eq!(*directory_node.children.get("subtree").unwrap(), 5);
        }

        let complex_subtree: Tree = utils::load_json(&tree0_path)?;
        tree.add_tree(complex_subtree, 0)?;

        if let Node::Directory(directory_node) = tree.get(6).expect("Expected a node") {
            assert_eq!(*directory_node.children.get("file0").unwrap(), 7);
            assert_eq!(*directory_node.children.get("dir1").unwrap(), 8);
        }

        Ok(())
    }
}
