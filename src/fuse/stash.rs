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

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    fuse::{cache::SnapshotTreeCache, fs::Inode},
    global::ID,
    repository::RepositoryBackend,
};

#[derive(Debug)]
pub(super) enum FsNode {
    Root,
    Dir {
        name: String,
        tree_id: Option<ID>,
        parent_ino: Inode,
    },
    TreeNode {
        name: String,
        tree_id: Option<ID>,
        node_idx: usize,
        parent_ino: Inode,
    },
}

pub(super) struct Stash {
    repo: Arc<dyn RepositoryBackend>,

    nodes: BTreeMap<Inode, FsNode>,
    path_cache: BTreeMap<(Inode, String), Inode>,
    tree_cache: SnapshotTreeCache,
}
