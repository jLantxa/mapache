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

use {
    anyhow::{Context, Result},
    std::{
        fs::{self, OpenOptions},
        io::Write,
        path::Path,
    },
};

#[cfg(unix)]
use {
    anyhow::bail,
    std::{
        fs::Permissions,
        os::unix::fs::{PermissionsExt, symlink},
    },
};

// Add filetime imports
use filetime::{FileTime, set_file_times}; // Import FileTime and set_file_times

use crate::repository::{
    RepositoryBackend,
    tree::{Node, NodeType},
};
#[cfg(not(unix))]
use crate::ui;

/// Restores a node to the specified destination path.
pub fn restore_node_to_path(
    repo: &dyn RepositoryBackend,
    node: &Node,
    dst_path: &Path,
) -> Result<()> {
    match node.node_type {
        NodeType::File => {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "Could not create parent directories for file '{}'",
                        dst_path.display()
                    )
                })?;
            }

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

            // Restore metadata after content is written
            restore_node_metadata(node, dst_path)?;
        }

        NodeType::Directory => {
            std::fs::create_dir_all(dst_path)
                .with_context(|| format!("Could not create directory '{}'", dst_path.display()))?;
            // Restore metadata after directory is created
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
                use crate::ui;

                ui::cli::log_warning(&format!(
                    "Symlink restoration not supported on this operating system: '{}'",
                    dst_path.display()
                ));
            }
        }

        NodeType::BlockDevice => {
            #[cfg(unix)]
            ui::cli::log_warning(&format!(
                "Restoration of block device '{}' not supported yet.",
                dst_path.display()
            ));
            #[cfg(not(unix))]
            ui::cli::log_warning(&format!(
                "Block device restoration not supported on this operating system: '{}'",
                dst_path.display()
            ));
        }

        NodeType::CharDevice => {
            #[cfg(unix)]
            ui::cli::log_warning(&format!(
                "Restoration of character device '{}' not supported yet.",
                dst_path.display()
            ));
            #[cfg(not(unix))]
            ui::cli::log_warning(&format!(
                "Character device restoration not supported on this operating system: '{}'",
                dst_path.display()
            ));
        }

        NodeType::Fifo => {
            #[cfg(unix)]
            ui::cli::log_warning(&format!(
                "Restoration of FIFO (named pipe) '{}' not supported yet.",
                dst_path.display()
            ));
            #[cfg(not(unix))]
            ui::cli::log_warning(&format!(
                "FIFO restoration not supported on this operating system: '{}'",
                dst_path.display()
            ));
        }

        NodeType::Socket => {
            #[cfg(unix)]
            ui::cli::log_warning(&format!(
                "Restoration of socket '{}' not supported yet.",
                dst_path.display()
            ));
            #[cfg(not(unix))]
            ui::cli::log_warning(&format!(
                "Socket restoration not supported on this operating system: '{}'",
                dst_path.display()
            ));
        }
    }

    Ok(())
}

/// Restores the metadata of a node to the specified destination path.
fn restore_node_metadata(node: &Node, dst_path: &Path) -> Result<()> {
    // Set file times
    if let Some(modified_time) = node.metadata.modified_time {
        let ft_mtime = FileTime::from(modified_time);
        let ft_atime = node
            .metadata
            .accessed_time
            .map_or(ft_mtime, |atime| FileTime::from(atime));

        set_file_times(dst_path, ft_atime, ft_mtime)
            .with_context(|| format!("Could not set modified time for '{}'", dst_path.display()))?;
    }

    // Unix-specific metadata (mode, uid, gid)
    #[cfg(unix)]
    {
        // Set file permissions (mode)
        if let Some(mode) = node.metadata.mode {
            let permissions = Permissions::from_mode(mode);
            if let Err(e) = std::fs::set_permissions(dst_path, permissions) {
                bail!(
                    "Could not set permissions for '{}': {}. This may not be supported for all node types (e.g. symlinks).",
                    dst_path.display(),
                    e.to_string()
                );
            }
        }

        // Set owner (uid) and group (gid)
        let uid = node.metadata.owner_uid.map(|u| u as u32);
        let gid = node.metadata.owner_gid.map(|g| g as u32);

        if uid.is_some() || gid.is_some() {
            if let Err(e) = std::os::unix::fs::chown(dst_path, uid, gid) {
                bail!(
                    "Could not set owner/group for '{}': {}. This operation often requires elevated privileges (e.g., root) and may not be supported for all node types (e.g. symlinks).",
                    dst_path.display(),
                    e.to_string()
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use {
        chrono::{Duration, Local},
        std::time::SystemTime,
    };

    use super::*;

    #[cfg(unix)]
    #[test]
    fn test_restore_mtime() -> Result<()> {
        use std::fs::File;

        let temp_path = testing::create_tmp_dir()?;

        let file_path = temp_path.join("file.txt");
        std::fs::write(&file_path, b"Mapachito").expect("Expected to write to file");
        let node = Node::from_path(&file_path)?;

        // Change mtime to 1 day before now
        let prev_mtime: SystemTime = (Local::now() - Duration::days(1)).into();
        let ft_mtime = FileTime::from(prev_mtime);
        let ft_atime = node
            .metadata
            .modified_time
            .map_or(ft_mtime, |atime| FileTime::from(atime));

        set_file_times(&file_path, ft_atime, ft_mtime).with_context(|| {
            format!("Could not set modified time for '{}'", file_path.display())
        })?;

        restore_node_metadata(&node, &file_path)?;

        assert_eq!(
            node.metadata.modified_time.unwrap(),
            file_path.symlink_metadata().unwrap().modified().unwrap()
        );

        Ok(())
    }
}
