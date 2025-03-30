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

use std::collections::BTreeMap;

use blake3::Hasher;
use serde::{Deserialize, Serialize};

use crate::utils::hashing::{Hash, Hashable};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryNode {
    pub name: String,
    pub metadata: Option<DirectoryMetadata>,
    pub files: BTreeMap<String, FileEntry>,
    pub children: BTreeMap<String, DirectoryNode>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub name: String,
    pub metadata: Option<FileMetadata>,
    pub chunks: Vec<Hash>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableDirectoryNode {
    pub name: String,
    pub metadata: Option<DirectoryMetadata>,
    pub files: Vec<FileEntry>,
    pub children: Vec<Hash>,
}

impl Hashable for SerializableDirectoryNode {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::new();

        hasher.update(self.name.as_bytes());

        if let Some(meta) = &self.metadata {
            hasher.update(meta.hash().as_bytes());
        }

        for file in &self.files {
            hasher.update(file.hash().as_bytes());
        }

        for child_hash in &self.children {
            hasher.update(child_hash.as_bytes());
        }

        let hash = hasher.finalize();
        format!("{}", hash)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirectoryMetadata {}

impl Hashable for DirectoryMetadata {
    fn hash(&self) -> Hash {
        let hasher = Hasher::new();

        let hash = hasher.finalize();
        format!("{}", hash)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileMetadata {}

impl Hashable for FileMetadata {
    fn hash(&self) -> Hash {
        let hasher = Hasher::new();

        let hash = hasher.finalize();
        format!("{}", hash)
    }
}
