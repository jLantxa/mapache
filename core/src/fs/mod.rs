use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::{Component, Path, PathBuf},
};

use anyhow::Result;
use rustc_hash::{FxHashMap, FxHashSet};

pub mod filter;
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

// --- Path Utilities ---

/// Calculates the longest common prefix for a set of paths.
/// Returns an empty PathBuf if the input is empty or if no common prefix exists.
/// If `strict_prefix` is true, the LCP of a single path is itself,
/// otherwise, the LCP is the parent:
///
/// - `true`: a/b/c -> a/b/c
/// - `false`: a/b/c -> a/b
pub fn calculate_lcp(paths: &[PathBuf], strict_prefix: bool) -> PathBuf {
    if paths.is_empty() {
        return PathBuf::new();
    } else if paths.len() == 1 {
        if strict_prefix {
            return paths[0].clone();
        } else {
            return extract_parent(&paths[0]).expect("Path should have a parent");
        }
    }

    let mut common_prefix = PathBuf::new();
    let mut iterators: Vec<_> = paths.iter().map(|p| p.components()).collect();

    'outer: loop {
        let current_components: Vec<_> = iterators.iter_mut().map(|it| it.next()).collect();

        if current_components.iter().any(Option::is_none) {
            break 'outer;
        }

        let first_comp = current_components[0].as_ref().unwrap();
        let all_match = current_components[1..]
            .iter()
            .all(|comp_opt| comp_opt.as_ref().is_some_and(|comp| comp.eq(first_comp)));

        if all_match {
            common_prefix.push(first_comp);
        } else {
            break 'outer;
        }
    }

    common_prefix
}

/// Extracts the parent path of a given path.
/// Returns `None` if the path has no parent (e.g., "/" or "file.txt" in current dir).
#[inline]
pub fn extract_parent(path: &Path) -> Option<PathBuf> {
    path.parent().map(PathBuf::from)
}

/// For each directory between `root` and any of the `paths`,
/// return how many *distinct* direct children each intermediate directory has,
/// and how many *distinct* direct children the `root` itself has.
///
/// The returned `BTreeMap` keys are the intermediate parent paths.
/// The `usize` value is the count of distinct direct children under that parent.
/// The first element of the tuple is the count of distinct direct children under `root`.
pub fn get_intermediate_paths(root: &Path, paths: &[PathBuf]) -> (usize, BTreeMap<PathBuf, usize>) {
    // parent dir -> set of *child names* (single component) directly under that parent
    let mut children_map: FxHashMap<PathBuf, FxHashSet<OsString>> = FxHashMap::default();
    let mut unique_root_children: FxHashSet<OsString> = FxHashSet::default();

    for full_path in paths {
        let mut cur = full_path.as_path();
        let mut direct_root_child_name: Option<OsString> = None;

        while let Some(parent) = cur.parent() {
            // Stop when we reached (or crossed) root.
            // Note: this relies on your existing ordering semantics for normalized absolute paths.
            if parent <= root {
                if parent == root
                    && let Some(name) = cur.file_name()
                {
                    direct_root_child_name = Some(name.to_os_string());
                }
                break;
            }

            // Insert the child name under its parent.
            if let Some(name) = cur.file_name() {
                children_map
                    .entry(parent.to_path_buf())
                    .or_default()
                    .insert(name.to_os_string());
            }

            cur = parent;
        }

        if let Some(name) = direct_root_child_name {
            unique_root_children.insert(name);
        }
    }

    let root_children_count = unique_root_children.len();

    // Convert to BTreeMap for sorted keys (preserves your original return type / ordering)
    let mut intermediate_counts = BTreeMap::new();
    intermediate_counts.extend(children_map.into_iter().map(|(p, set)| (p, set.len())));

    (root_children_count, intermediate_counts)
}

/// Abbreviates a path to fit within `max_len`, keeping the first and last components.
///
/// Long paths are shortened by replacing middle components with `"..."`.
/// Always preserves the root (if any) and the filename.
pub fn abbreviate_path(path: &Path, max_len: usize) -> String {
    let path_str = path.to_string_lossy();
    if path_str.is_empty() {
        return String::new();
    }

    // If it already fits, return as-is
    if path_str.len() <= max_len {
        return path_str.into_owned();
    }

    let components: Vec<_> = path.components().collect();
    if components.len() <= 2 {
        return path_str.into_owned();
    }

    let first = components[0].as_os_str().to_string_lossy();
    let last = components.last().unwrap().as_os_str().to_string_lossy();
    let ellipsis = "...";

    // base_res_len handles: [first][sep?][...][sep][last]
    let needs_sep_after_first = !first.ends_with(std::path::is_separator);
    let mut base_res_len = first.len() + ellipsis.len() + last.len() + 1;
    if needs_sep_after_first {
        base_res_len += 1;
    }

    if base_res_len > max_len {
        return path_str.into_owned();
    }

    let mut middle_parts = Vec::new();
    let mut current_len = base_res_len;

    // --- FIX START ---
    // If we have a Windows prefix followed by a root (e.g., C:\),
    // we skip the RootDir component to avoid double slashes.
    let skip_count = if components.len() > 1
        && matches!(components[0], std::path::Component::Prefix(_))
        && matches!(components[1], std::path::Component::RootDir)
    {
        2
    } else {
        1
    };

    let middle_count = components.len().saturating_sub(1 + skip_count);

    for component in components.iter().skip(skip_count).take(middle_count) {
        // --- FIX END ---
        let comp_str = component.as_os_str().to_string_lossy();
        let added_len = comp_str.len() + 1; // component + separator

        if current_len + added_len <= max_len {
            current_len += added_len;
            middle_parts.push(comp_str);
        } else {
            // We've reached the length limit; the rest will be replaced by ellipsis.
            break;
        }
    }

    let mut result = String::with_capacity(current_len);
    result.push_str(&first);

    for comp in middle_parts {
        if !result.ends_with(std::path::is_separator) {
            result.push(std::path::MAIN_SEPARATOR);
        }
        result.push_str(&comp);
    }

    if !result.ends_with(std::path::is_separator) {
        result.push(std::path::MAIN_SEPARATOR);
    }
    result.push_str(ellipsis);

    if !result.ends_with(std::path::is_separator) {
        result.push(std::path::MAIN_SEPARATOR);
    }
    result.push_str(&last);

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
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

    #[test]
    fn test_calculate_lcp() {
        let paths: Vec<PathBuf> = vec![];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::new());

        let paths = vec![PathBuf::from("/home/user/docs")];
        assert_eq!(
            calculate_lcp(&paths, true),
            PathBuf::from("/home/user/docs")
        );

        let paths = vec![PathBuf::from("/home/user/docs")];
        assert_eq!(calculate_lcp(&paths, false), PathBuf::from("/home/user"));

        let paths = vec![
            PathBuf::from("/home/user/a"),
            PathBuf::from("/home/user/b/file.txt"),
            PathBuf::from("/home/user/c"),
        ];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::from("/home/user"));

        let paths = vec![
            PathBuf::from("/home/user/docs"),
            PathBuf::from("/etc"),
            PathBuf::from("/var/log"),
        ];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::from("/"));

        let paths = vec![
            PathBuf::from("a/b/c"),
            PathBuf::from("a/b/d"),
            PathBuf::from("a/b"),
        ];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::from("a/b"));

        let paths = vec![PathBuf::from("a/b"), PathBuf::from("x/y")];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::new());

        let paths = vec![PathBuf::from("/home/user/a"), PathBuf::from("a")];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::new());
    }

    #[test]
    fn test_get_intermediate_paths() {
        let root = PathBuf::from("/");
        let paths = vec![
            PathBuf::from("/a/b/c"),
            PathBuf::from("/a/b/d"),
            PathBuf::from("/a/e"),
        ];
        let (root_children_count, intermediate_paths) = get_intermediate_paths(&root, &paths);
        let mut expected = BTreeMap::new();
        expected.insert(PathBuf::from("/a"), 2);
        expected.insert(PathBuf::from("/a/b"), 2);
        assert_eq!(root_children_count, 1);
        assert_eq!(intermediate_paths, expected);

        // Test with root as a subpath
        let root = PathBuf::from("/a");
        let paths = vec![
            PathBuf::from("/a/b/c"),
            PathBuf::from("/a/b/d"),
            PathBuf::from("/a/e"),
        ];
        let (root_children_count, intermediate_paths) = get_intermediate_paths(&root, &paths);
        let mut expected = BTreeMap::new();
        expected.insert(PathBuf::from("/a/b"), 2);
        assert_eq!(root_children_count, 2); // 'b' and 'e' are direct children of '/a'
        assert_eq!(intermediate_paths, expected);
    }

    #[cfg(unix)]
    #[test]
    fn test_abbreviate_path_unix() {
        // Note: On Unix, the first component of "/home/user" is "/"

        assert_eq!(abbreviate_path(Path::new("short/path"), 5), "short/path");

        let path = Path::new("/home/user/some/path/to/a/file.txt");
        // first="/" + middle="home" + "..." + last="file.txt" = "/home/.../file.txt"
        assert_eq!(abbreviate_path(path, 20), "/home/.../file.txt");
        assert_eq!(abbreviate_path(path, 30), "/home/user/some/.../file.txt");

        let long_path_str = "/home/user/projects/backup_tool/src/core/permissions.rs";
        let long_path = Path::new(long_path_str);

        assert_eq!(
            abbreviate_path(long_path, 30),
            "/home/user/.../permissions.rs"
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_abbreviate_path_windows() {
        let path = Path::new(r"C:\Users\Admin\Documents\file.txt");
        let result = abbreviate_path(path, 20);
        // Should handle C:\ without doubling the backslash
        assert_eq!(result, r"C:\...\file.txt");
        assert!(!result.contains(r"\\"));

        let path = Path::new(r"C:\Users\Admin\Documents\file.txt");
        let result = abbreviate_path(path, 25);
        // Should handle C:\ without doubling the backslash
        assert_eq!(result, r"C:\Users\...\file.txt");
        assert!(!result.contains(r"\\"));

        let path = Path::new(r"C:\Users\Admin\Documents\file.txt");
        let result = abbreviate_path(path, 30);
        // Should handle C:\ without doubling the backslash
        assert_eq!(result, r"C:\Users\Admin\...\file.txt");
        assert!(!result.contains(r"\\"));
    }
}
