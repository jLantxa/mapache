use std::path::{Component, Path, PathBuf};

use anyhow::Result;

pub mod node;
pub mod tree;

pub fn path_exists(path: &Path) -> bool {
    path.symlink_metadata().is_ok()
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
                // Check if the last component added is a "Normal" directory.
                // We only pop if we have something like "dir/.."
                let is_last_popable = matches!(components.last(), Some(Component::Normal(_)));

                if is_last_popable {
                    components.pop();
                } else {
                    // If the last component is the Root (/) or a Prefix (C:),
                    // a '..' is ignored because you can't go above the filesystem root.
                    // If the path is relative or we are already at a series of '..',
                    // we push the '..' to keep the path valid.
                    let is_at_root = components
                        .last()
                        .is_some_and(|c| matches!(c, Component::RootDir | Component::Prefix(_)));

                    if !is_at_root {
                        components.push(component);
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_get_absolute_normalized_path() -> Result<()> {
        let cwd = env::current_dir()?;

        // Define test cases as (input_path, expected_path)
        let mut cases = vec![
            // Basic relative path normalization
            ("foo/bar/../baz", cwd.join("foo").join("baz")),
            ("./test/././file.txt", cwd.join("test").join("file.txt")),
            // Parent directory traversal at CWD level
            ("..", cwd.parent().unwrap_or(&cwd).to_path_buf()),
            // Redundant slashes
            // Note: Components() collapses these, but we join properly for the test
            ("foo//bar///baz/", cwd.join("foo").join("bar").join("baz")),
            // 4. Empty and dot paths
            (".", cwd.clone()),
            ("", cwd.clone()),
        ];

        // Platform-specific absolute paths
        if cfg!(windows) {
            // On Windows, we test with explicit Drive letters.
            // Note: We use PathBuf::from to avoid prepending CWD to already absolute paths.
            cases.extend(vec![
                ("C:\\\\prj\\\\.\\\\foo", PathBuf::from("C:\\prj\\foo")),
                (
                    "C:/Users//Admin/../Public",
                    PathBuf::from("C:\\Users\\Public"),
                ),
                ("C:\\..\\..\\..\\Windows", PathBuf::from("C:\\Windows")),
            ]);
        } else {
            cases.extend(vec![
                ("/usr/local/../bin", PathBuf::from("/usr/bin")),
                ("/../../../../etc/passwd", PathBuf::from("/etc/passwd")),
                ("///etc//hosts", PathBuf::from("/etc/hosts")),
            ]);
        }

        for (input, expected) in cases {
            let result = get_absolute_normalized_path(Path::new(input))
                .map_err(|e| format!("Failed to process '{}': {}", input, e))
                .unwrap();

            // We compare string representations or normalized PathBufs
            // to avoid issues with UNC prefixes in some Windows environments.
            assert_eq!(
                result, expected,
                "\nNormalization mismatch!\nInput:    {:?}\nExpected: {:?}\nActual:   {:?}",
                input, expected, result
            );
        }

        Ok(())
    }

    #[test]
    fn test_normalization_is_lexical_only() -> Result<()> {
        // This confirms the function does not check if the path actually exists.
        // It should work even with fictional directories.
        let path = Path::new("non_existent_7788/../exists_only_in_memory");
        let result = get_absolute_normalized_path(path)?;

        assert!(result.to_string_lossy().contains("exists_only_in_memory"));
        assert!(!result.to_string_lossy().contains("non_existent_7788"));
        Ok(())
    }
}
