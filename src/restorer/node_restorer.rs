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
    fs::{File, FileTimes, OpenOptions, Permissions},
    io::Write,
    path::Path,
};

#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, symlink};

use anyhow::{Context, Result, bail};

use crate::{
    repository::{
        RepositoryBackend,
        tree::{Node, NodeType},
    },
    ui,
};

/// Restores a node to the specified destination path.
pub fn restore_node_to_path(
    repo: &dyn RepositoryBackend,
    node: &Node,
    dst_path: &Path,
) -> Result<()> {
    match node.node_type {
        NodeType::File => {
            let mut dst_file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(dst_path)
                .with_context(|| {
                    format!("Could not create destination file '{}'", dst_path.display())
                })?;

            let blocks = node
                .blobs
                .as_ref()
                .expect("File Node must have contents (even if empty)");

            for (index, chunk_hash) in blocks.iter().enumerate() {
                let chunk_data = repo.load_blob(&chunk_hash).with_context(|| {
                    format!(
                        "Could not load block #{} ({}) for restoring file '{}'",
                        index + 1,
                        chunk_hash,
                        dst_path.display()
                    )
                })?;

                dst_file.write_all(&chunk_data).with_context(|| {
                    format!(
                        "Could not restore block #{} ({}) to file '{}'",
                        index + 1,
                        chunk_hash,
                        dst_path.display()
                    )
                })?;
            }

            restore_node_metadata(node, dst_path)?;
        }

        NodeType::Directory => {
            std::fs::create_dir_all(dst_path)?;
            restore_node_metadata(node, dst_path)?;
        }

        NodeType::Symlink => {
            #[cfg(unix)]
            {
                let target = node
                    .symlink_target
                    .as_ref()
                    .expect("Symlink Node must have a target path");
                symlink(target, dst_path).with_context(|| {
                    format!(
                        "Could not create symlink '{}' pointing to '{}'",
                        dst_path.display(),
                        target.display()
                    )
                })?;
            }
            #[cfg(not(unix))]
            {
                ui::cli::log_warning("Symlink restoration not supported on this operating system");
            }

            // No need to restore metadata for symlinks?
        }

        NodeType::BlockDevice => {
            // TODO: Implement BlockDevice restoration
            #[cfg(unix)]
            {
                ui::cli::log_warning("Block device restoration not supported yet");
            }

            #[cfg(not(unix))]
            ui::cli::log_warning("Block device restoration not supported on this operating system");
        }

        NodeType::CharDevice => {
            // TODO: Implement CharDevice restoration
            #[cfg(unix)]
            {
                ui::cli::log_warning("Char device restoration not supported yet");
            }

            #[cfg(not(unix))]
            ui::cli::log_warning("Char device restoration not supported on this operating system");
        }

        NodeType::Fifo => {
            // TODO: Implement Fifo restoration
            #[cfg(unix)]
            {
                ui::cli::log_warning("FIFO restoration not supported yet");
            }

            #[cfg(not(unix))]
            ui::cli::log_warning("FIFO device restoration not supported on this operating system");
        }

        NodeType::Socket => {
            // TODO: Implement Socket restoration
            #[cfg(unix)]
            {
                ui::cli::log_warning("Socket restoration not supported yet");
            }

            #[cfg(not(unix))]
            ui::cli::log_warning("Socket restoration not supported on this operating system");
        }
    }

    Ok(())
}

/// Restores the metadata of a node to the specified destination path.
fn restore_node_metadata(node: &Node, dst_path: &Path) -> Result<()> {
    if let Some(modified_time) = node.metadata.modified_time {
        let dst_file = File::open(dst_path)?;
        let filetimes = FileTimes::new().set_modified(modified_time);
        dst_file
            .set_times(filetimes)
            .with_context(|| format!("Could not set file times to path {:?}", dst_path))?;
    }

    #[cfg(unix)]
    if let Some(mode) = node.metadata.mode {
        let permissions = Permissions::from_mode(mode);
        if let Err(e) = std::fs::set_permissions(dst_path, permissions) {
            bail!(
                "Could not set permissions for '{}': {}",
                dst_path.display(),
                e.to_string()
            );
        }
    }

    #[cfg(unix)]
    {
        let uid = node.metadata.owner_uid.map(|u| u as u32); // Option<u32>
        let gid = node.metadata.owner_gid.map(|g| g as u32); // Option<u32>

        // Only attempt chown if either uid or gid is explicitly specified
        if uid.is_some() || gid.is_some() {
            if let Err(e) = std::os::unix::fs::chown(dst_path, uid, gid) {
                bail!(
                    "Could not set owner/group for '{}': {}. This operation often requires elevated privileges (e.g., root).",
                    dst_path.display(),
                    e.to_string()
                );
            }
        }
    }

    Ok(())
}
