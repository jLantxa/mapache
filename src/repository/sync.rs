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

use anyhow::Result;
use std::path::{Path, PathBuf};

use crate::{backend::StorageBackend, repository::repo::LOCKS_DIR, ui, utils};

#[derive(Debug, Clone, PartialEq)]
pub enum BackendNode {
    File(PathBuf),
    Dir(PathBuf),
}

impl BackendNode {
    pub fn path(&self) -> &Path {
        match self {
            BackendNode::File(path) => path,
            BackendNode::Dir(path) => path,
        }
    }
}

/// Synchronize a repository to a destination backend.
pub fn sync_repository(
    src_backend: &dyn StorageBackend,
    dst_backend: &dyn StorageBackend,
    delete: bool,
) -> Result<()> {
    ui::cli::log!("Reading source repository...");
    let mut src_nodes = read_backend_dir(src_backend, &PathBuf::new())?;
    let mut dst_nodes = read_backend_dir(dst_backend, &PathBuf::new())?;

    let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
    let reverse_cmp = |n0: &BackendNode, n1: &BackendNode| n1.path().cmp(n0.path());

    src_nodes.sort_unstable_by(forward_cmp);
    dst_nodes.sort_unstable_by(forward_cmp);

    let mut src_iter = src_nodes.into_iter().peekable();
    let mut dst_iter = dst_nodes.into_iter().peekable();

    let mut to_copy: Vec<BackendNode> = Vec::new();
    let mut to_delete: Vec<BackendNode> = Vec::new();

    ui::cli::log!("Calculating differences...");
    loop {
        match (src_iter.peek(), dst_iter.peek()) {
            (Some(src_node), Some(dst_node)) => {
                let ordering = src_node.path().cmp(dst_node.path());
                match ordering {
                    std::cmp::Ordering::Less => {
                        // src_node is in src but not in dst
                        to_copy.push(src_iter.next().unwrap());
                    }
                    std::cmp::Ordering::Greater => {
                        // dst_node is in dst but not in src
                        to_delete.push(dst_iter.next().unwrap());
                    }
                    std::cmp::Ordering::Equal => {
                        // All objects in the repository are named by hash, so files with identical
                        // paths must have the same content.
                        src_iter.next();
                        dst_iter.next();
                    }
                }
            }
            (Some(_src_node), None) => {
                // Remaining src_nodes are all new
                to_copy.push(src_iter.next().unwrap());
            }
            (None, Some(_dst_node)) => {
                // Remaining dst_nodes are all to be deleted
                to_delete.push(dst_iter.next().unwrap());
            }
            (None, None) => {
                // Both iterators exhausted
                break;
            }
        }
    }

    ui::cli::log!(
        "{} to copy",
        utils::format_count(to_copy.len(), "item", "items")
    );
    ui::cli::log!(
        "{} to delete",
        utils::format_count(to_delete.len(), "item", "items")
    );

    // TODO: Display progress bars

    // Copy nodes to dst
    for node in to_copy {
        // Copy files from src to dst.

        // TODO:
        // For better performance, we should implement buffered I/O in the backend
        // and transfer the files in small chunks.

        match node {
            BackendNode::Dir(path) => dst_backend.create_dir_all(&path)?,
            BackendNode::File(path) => {
                let data = src_backend.read(&path)?;
                dst_backend.write(&path, &data)?;
            }
        }
    }

    // Delete obsolete objects
    if delete {
        // It's important to delete files before directories if a directory contains files.
        // Sorting in reverse order ensures files within a directory are processed before the directory itself.
        to_delete.sort_unstable_by(reverse_cmp); // Reverse sort for deletion
        for node in to_delete {
            match node {
                BackendNode::File(path) => dst_backend.remove_file(&path),
                BackendNode::Dir(path) => dst_backend.remove_dir_all(&path),
            }?
        }
    }

    Ok(())
}

/// Recursively list all files and directories in a backend
pub fn read_backend_dir(backend: &dyn StorageBackend, path: &Path) -> Result<Vec<BackendNode>> {
    let mut nodes = Vec::new();

    let root_nodes = backend.read_dir(path)?;
    for sub_path in root_nodes {
        // Ignore 'locks' directory.
        // The sync command will acquire a lock, but the locks must not be synchronized.
        if sub_path
            .file_name()
            .map(|name| name == LOCKS_DIR)
            .unwrap_or(false)
            && backend.is_dir(&sub_path)
        {
            continue;
        }

        if backend.is_file(&sub_path) {
            nodes.push(BackendNode::File(sub_path.to_path_buf()));
        } else if backend.is_dir(&sub_path) {
            nodes.push(BackendNode::Dir(sub_path.to_path_buf()));
            let mut sub_nodes = read_backend_dir(backend, &sub_path)?;
            nodes.append(&mut sub_nodes);
        }
    }

    Ok(nodes)
}
