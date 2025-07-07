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

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

use crate::{
    global::ID,
    repository::{RepositoryBackend, tree::Tree},
};

pub(super) struct SnapshotTreeCache {
    repo: Arc<dyn RepositoryBackend>,
    trees: HashMap<ID, Tree>,
}

impl SnapshotTreeCache {
    pub(super) fn new(repo: Arc<dyn RepositoryBackend>) -> Self {
        Self {
            repo,
            trees: Default::default(),
        }
    }

    pub(super) fn lookup_or_load(&mut self, id: &ID) -> Result<&Tree> {
        Ok(self.trees.entry(id.clone()).or_insert_with(|| {
            let tree_blob = self
                .repo
                .load_blob(id)
                .expect(&format!("Failed to load tree {}", id.to_hex()));
            serde_json::from_slice(&tree_blob)
                .expect(&format!("Failed to serialize tree {}", id.to_hex()))
        }))
    }
}
