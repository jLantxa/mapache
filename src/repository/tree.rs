/*
 * [backup] is an incremental backup tool
 * Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use std::{collections::BTreeMap, path::Path};

use blake3::Hasher;
use serde::{Deserialize, Serialize};

use crate::{
    filesystem::{DirectoryMetadata, FileMetadata},
    utils::hashing::{Hash, Hashable},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub name: String,
    pub metadata: Option<FileMetadata>,
    pub chunks: Vec<Hash>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryNode {
    pub name: String,
    pub metadata: Option<DirectoryMetadata>,
    pub files: BTreeMap<String, FileEntry>,
    pub children: BTreeMap<String, DirectoryNode>,
}

#[derive(Debug)]
pub enum TreeNode<'a> {
    File(&'a FileEntry),
    Directory(&'a DirectoryNode),
}

impl FileEntry {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            metadata: None,
            chunks: Vec::new(),
        }
    }
}

impl Hashable for FileEntry {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::new();

        hasher.update(self.name.as_bytes());

        if let Some(meta) = &self.metadata {
            hasher.update(meta.hash().as_bytes());
        }

        for chunk in &self.chunks {
            hasher.update(chunk.as_bytes());
        }

        let hash = hasher.finalize();
        format!("{}", hash)
    }
}

impl DirectoryNode {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            metadata: None,
            files: BTreeMap::new(),
            children: BTreeMap::new(),
        }
    }

    pub fn add_file(&mut self, file_entry: &FileEntry) {
        self.files
            .insert(file_entry.name.clone(), file_entry.clone());
    }

    pub fn add_dir(&mut self, dir: &DirectoryNode) {
        self.children.insert(dir.name.clone(), dir.clone());
    }

    /// Find a tree element (file or directory in the tree)
    pub fn find<'a>(&'a self, path: &Path) -> Option<TreeNode<'a>> {
        let mut current = self;
        let mut stack = Vec::new();
        let mut components = path.components().peekable();

        while let Some(component) = components.next() {
            match component {
                std::path::Component::CurDir => continue, // Ignore "."
                std::path::Component::ParentDir => {
                    //
                    if let Some(parent) = stack.pop() {
                        current = parent;
                    } else {
                        return None; // We can't go above the root
                    }
                }
                std::path::Component::Normal(os_str) => {
                    let name = os_str.to_str()?;

                    // Last component: check if it's a file or directory
                    if components.peek().is_none() {
                        return current
                            .files
                            .get(name)
                            .map(TreeNode::File)
                            .or_else(|| current.children.get(name).map(TreeNode::Directory));
                    }

                    // Traverse deeper if it's a directory
                    if let Some(dir) = current.children.get(name) {
                        stack.push(current);
                        current = dir;
                    } else {
                        return None;
                    }
                }
                _ => continue, // Ignore root and prefix components
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use anyhow::Result;

    use crate::{repository::tree::TreeNode, testing, utils};

    use super::DirectoryNode;

    /// Test finding an element inside the DirectoryNode
    #[test]
    fn test_find_in_tree() -> Result<()> {
        let tree_path = Path::new(testing::TEST_DATA_PATH).join("tree0.json");
        let root: DirectoryNode = utils::json::load_json(&tree_path)?;

        // Matches
        let dir0 = root.find(Path::new("dir0"));
        assert!(
            matches!(dir0, Some(TreeNode::Directory(_))),
            "Expected dir0 to be a Directory"
        );

        let file1 = root.find(Path::new("dir0/file1"));
        assert!(
            matches!(file1, Some(TreeNode::File(_))),
            "Expected dir0/file1 to be a File"
        );

        let file2 = root.find(Path::new("dir0/file2"));
        assert!(
            matches!(file2, Some(TreeNode::File(_))),
            "Expected dir0/file2 to be a File"
        );

        let file0 = root.find(Path::new("file0"));
        assert!(
            matches!(file0, Some(TreeNode::File(_))),
            "Expected file0 to be a File"
        );

        // Not found
        let dir1 = root.find(Path::new("dir1"));
        assert!(dir1.is_none(), "Expected dir1 to be None");

        let file_x = root.find(Path::new("dir0/fileX"));
        assert!(file_x.is_none(), "Expected dir0/fileX to be None");

        let deep_path = root.find(Path::new("dir0/does_not_exist"));
        assert!(
            deep_path.is_none(),
            "Expected dir0/does_not_exist to be None"
        );

        let abs_path = root.find(Path::new("/dir0/file0"));
        assert!(abs_path.is_none(), "Expected /dir0/file0 to be None");

        Ok(())
    }
}
