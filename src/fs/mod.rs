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

use std::path::{Component, Path, PathBuf};

use anyhow::Result;

pub mod node;
pub mod tree;

pub fn path_exists(path: &Path) -> bool {
    path.symlink_metadata().is_ok()
}

pub fn path_exists_follow_symlink(path: &Path) -> bool {
    path.exists()
}

/// Returns the absolute, normalized path of a node without following symlinks.
///
/// This function is a lexical (string-based) path resolver. It resolves
/// `.` (current directory) and `..` (parent directory) components and handles
/// relative paths by prepending the current working directory.
///
/// Unlike `std::fs::canonicalize`, this function does not perform any
/// filesystem access and will not resolve symbolic links.
///
/// This implementation is designed to work correctly on both Linux and Windows,
/// handling platform-specific path conventions like Windows drive letters and UNC paths.
pub fn get_absolute_normalized_path(path: &Path) -> Result<PathBuf> {
    // Get an absolute path.
    // If the path is not absolute, prepend the current working directory.
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    // Normalize the path components.
    let mut components = Vec::new();
    for component in absolute_path.components() {
        match component {
            // On Windows, the Prefix component handles drive letters (C:) and UNC paths (\\server\share).
            // This is a special case that must be handled separately to maintain correct paths.
            #[allow(unused_variables)]
            Component::Prefix(prefix) => {
                #[cfg(windows)]
                components.push(Component::Prefix(prefix));
            }

            // The root directory component (e.g., / on Linux, \ on Windows) should always be at the start.
            // We can push it directly.
            Component::RootDir => {
                components.push(component);
            }

            // A single dot (`.`) is a no-op in path normalization.
            Component::CurDir => {}

            // A parent directory (`..`) means we need to go up one level.
            Component::ParentDir => {
                // We should only pop if the last component is not the root or a parent directory itself.
                let is_last_popable = components
                    .last()
                    .map(|c| !matches!(c, Component::RootDir | Component::ParentDir))
                    .unwrap_or(false);

                if is_last_popable {
                    components.pop();
                } else {
                    // If we can't pop, for example, on a path like `/../a`, we must keep the `..`.
                    // This prevents escaping the root of the filesystem.
                    components.push(component);
                }
            }

            // A regular file or directory name is added to the list.
            Component::Normal(name) => {
                components.push(Component::Normal(name));
            }
        }
    }

    // Reconstruct the PathBuf from the cleaned components.
    let mut result = PathBuf::new();
    for component in components {
        result.push(component.as_os_str());
    }

    // Handle the edge case of an empty result, which can occur with an input like `.`
    if result.as_os_str().is_empty() {
        result = PathBuf::from(".");
    }

    Ok(result)
}
