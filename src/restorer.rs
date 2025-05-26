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

use std::{fs::OpenOptions, io::Write, path::Path};

use anyhow::{Context, Result};

use crate::repository::{
    RepositoryBackend,
    tree::{Node, NodeType},
};

pub fn restore_node(repo: &dyn RepositoryBackend, node: &Node, dst_path: &Path) -> Result<()> {
    match node.node_type {
        NodeType::File => {
            // TODO: Restore metadata
            let mut dst_file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(dst_path)
                .with_context(|| {
                    format!("Could not create destination file '{}'", dst_path.display())
                })?;

            let chunks = node
                .contents
                .as_ref()
                .expect("File Node must have contents (even if empty)");

            for (index, chunk_hash) in chunks.iter().enumerate() {
                let chunk_data = repo.load_blob(&chunk_hash).with_context(|| {
                    format!(
                        "Could not load chunk #{} ({}) for restoring file '{}'",
                        index + 1,
                        chunk_hash,
                        dst_path.display()
                    )
                })?;

                dst_file.write_all(&chunk_data).with_context(|| {
                    format!(
                        "Could not restore chunk #{} ({}) to file '{}'",
                        index + 1,
                        chunk_hash,
                        dst_path.display()
                    )
                })?;

                if let Some(mtime) = node.metadata.modified_time {
                    dst_file.set_modified(mtime)?;
                }
            }
        }
        NodeType::Directory => {
            // TODO: Restore metadata
            std::fs::create_dir_all(dst_path)?
        }
        NodeType::Symlink => todo!(),
        NodeType::BlockDevice => todo!(),
        NodeType::CharDevice => todo!(),
        NodeType::Fifo => todo!(),
        NodeType::Socket => todo!(),
    }

    Ok(())
}
