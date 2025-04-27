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

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use super::backend::{BlobId, TreeId};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub size: u64,
    pub mode: Option<u32>,
    pub created_time: Option<SystemTime>,
    pub modified_time: Option<SystemTime>,
    pub accessed_time: Option<SystemTime>,

    pub user: Option<String>,
    pub group: Option<String>,
    pub owner_uid: Option<u32>,
    pub owner_gid: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum NodeType {
    File,
    Directory,
    Simlink,
}

/// A Node, representing an item in a filesystem.
#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub node_type: NodeType,

    #[serde(flatten)]
    pub metadata: Metadata,

    pub contents: Option<Vec<BlobId>>,
    pub tree: Option<TreeId>,
}

/// A tree, represented as a collection of its immediate children nodes.
#[derive(Debug, Serialize, Deserialize)]
pub struct Tree {
    pub nodes: Vec<Node>,
}
