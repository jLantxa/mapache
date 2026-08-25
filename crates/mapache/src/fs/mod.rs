use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::{Component, Path, PathBuf},
};

use crate::{
    common::error::Result,
    utils::collections::{FxHashMap, FxHashSet},
};

pub mod filetime;
pub mod filter;
pub mod node;
pub mod scanner;
pub mod tree;

#[cfg(windows)]
fn convert_msys_path(path: &Path) -> Option<PathBuf> {
    let s = path.to_str()?;
    if s.starts_with('/')
        && s.len() >= 3
        && s.as_bytes()[1].is_ascii_alphabetic()
        && s.as_bytes()[2] == b'/'
    {
        let drive = s.as_bytes()[1].to_ascii_uppercase() as char;
        let rest = &s[3..];
        let mut result = format!("{}:\\", drive);
        result.push_str(&rest.replace('/', "\\"));
        Some(PathBuf::from(result))
    } else {
        None
    }
}

pub async fn path_exists(path: &Path) -> bool {
    tokio::fs::symlink_metadata(path).await.is_ok()
}

pub fn expand_tilde(path: &Path) -> PathBuf {
    let mut components = path.components();
    if let Some(Component::Normal(first)) = components.next()
        && first == "~"
        && let Ok(home) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE"))
    {
        let mut result = PathBuf::from(home);
        result.extend(components);
        return result;
    }
    path.to_path_buf()
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
/// handling platform-specific path conventions like Windows drive letters, UNC paths,
/// and MSYS2-style paths (e.g., /c/path → C:\path on Windows).
///
/// It also performs tilde expansion for paths starting with `~`.
pub fn get_absolute_normalized_path(path: &Path) -> Result<PathBuf> {
    let path_buf = expand_tilde(path);
    let path_ref = path_buf.as_path();

    #[cfg(windows)]
    let msys_path;
    #[cfg(windows)]
    let mut path_ref = path_ref;
    #[cfg(windows)]
    if let Some(p) = convert_msys_path(path_ref) {
        msys_path = p;
        path_ref = msys_path.as_path();
    }

    // Make absolute by joining with CWD if relative.
    let absolute_path = if path_ref.is_absolute() {
        path_ref.to_path_buf()
    } else {
        std::env::current_dir()?.join(path_ref)
    };

    // Lexically normalize components (resolve . and ..).
    let mut components = Vec::new();
    for component in absolute_path.components() {
        match component {
            Component::Prefix(prefix) => {
                #[cfg(windows)]
                components.push(Component::Prefix(prefix));
                #[cfg(not(windows))]
                let _ = prefix;
            }
            Component::RootDir => {
                components.push(component);
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if let Some(Component::Normal(_)) = components.last() {
                    components.pop();
                } else if !components
                    .last()
                    .is_some_and(|c| matches!(c, Component::RootDir | Component::Prefix(_)))
                {
                    components.push(component);
                }
            }
            Component::Normal(name) => {
                components.push(Component::Normal(name));
            }
        }
    }

    // 5. Reconstruct PathBuf.
    let mut result = PathBuf::new();
    for component in components {
        result.push(component.as_os_str());
    }

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
            return extract_parent(&paths[0]).unwrap_or_default();
        }
    }

    let mut common_prefix = PathBuf::new();
    let mut iterators: Vec<_> = paths.iter().map(|p| p.components()).collect();

    'outer: loop {
        let current_components: Vec<_> = iterators.iter_mut().map(|it| it.next()).collect();
        let first = match current_components[0] {
            Some(c) => c,
            None => break 'outer,
        };

        for c in &current_components[1..] {
            match c {
                Some(comp) if *comp == first => {}
                _ => break 'outer,
            }
        }
        common_prefix.push(first.as_os_str());
    }

    common_prefix
}

/// Returns the parent directory of a path as a PathBuf.
/// If the path has no parent, returns None.
pub fn extract_parent(path: &Path) -> Option<PathBuf> {
    path.parent().map(|p| p.to_path_buf())
}

/// A simplified path abbreviator for UI display.
///
/// If the path is longer than `max_len`, it will be shortened by keeping
/// the first component and the last component, and replacing the middle
/// with `...`.
///
/// Example: `/home/user/some/path/to/a/file.txt` -> `/home/.../file.txt`
pub fn abbreviate_path(path: &Path, max_len: usize) -> String {
    let path_str = path.to_string_lossy().to_string();
    if path_str.len() <= max_len {
        return path_str;
    }

    let components: Vec<_> = path.components().collect();
    if components.len() <= 2 {
        return path_str;
    }

    let first = components[0].as_os_str().to_string_lossy();
    let last = components
        .last()
        .expect("components has at least 3 elements (guarded by len check)")
        .as_os_str()
        .to_string_lossy();

    // Check if we can fit more components
    let mut left_idx = 1;
    let mut right_idx = components.len() - 2;

    let mut result_left = first.to_string();
    let mut result_right = last.to_string();

    // On Unix, if the first component is RootDir, the first char is already '/'
    // We don't want to add another slash when pushing the next component.
    if matches!(components[0], Component::RootDir) && components.len() > 3 {
        let second = components[1].as_os_str().to_string_lossy();
        // Result left is already "/", we just append second.
        result_left.push_str(&second);
        left_idx = 2;
    }

    let sep = if cfg!(windows) { "\\" } else { "/" };

    loop {
        let next_left = if left_idx <= right_idx {
            let s = components[left_idx].as_os_str().to_string_lossy();
            let mut res = result_left.clone();
            if !res.ends_with(['/', '\\']) && !matches!(components[left_idx], Component::RootDir) {
                res.push_str(sep);
            }
            res.push_str(&s);
            res
        } else {
            result_left.clone()
        };

        let next_right = if right_idx >= left_idx {
            let s = components[right_idx].as_os_str().to_string_lossy();
            let mut res = s.to_string();
            if !res.ends_with(['/', '\\']) {
                res.push_str(sep);
            }
            res.push_str(&result_right);
            res
        } else {
            result_right.clone()
        };

        // Calculation: result_left + sep + "..." + sep + result_right
        let total_len_left = next_left.len() + 5 + result_right.len();
        if total_len_left <= max_len && left_idx <= right_idx {
            result_left = next_left;
            left_idx += 1;
            continue;
        }

        let total_len_right = result_left.len() + 5 + next_right.len();
        if total_len_right <= max_len && right_idx >= left_idx {
            result_right = next_right;
            right_idx -= 1;
            continue;
        }

        break;
    }

    let mut final_left = result_left;
    if !final_left.ends_with(['/', '\\']) {
        final_left.push_str(sep);
    }

    let final_right = result_right;
    let mut final_ellipsis = "...".to_string();
    final_ellipsis.push_str(sep);

    format!("{}{}{}", final_left, final_ellipsis, final_right)
}

/// Build intermediate path maps for streaming.
///
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
            if !root.as_os_str().is_empty() && parent <= root {
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
        } else if root.as_os_str().is_empty() {
            // We reached the top (parent is None) and root is empty.
            // Treat 'cur' as a direct child of the virtual root.
            if let Some(name) = cur.file_name() {
                unique_root_children.insert(name.to_os_string());
            } else if let Some(comp) = cur.components().next() {
                // It's a root prefix like "C:\" or "/"
                unique_root_children.insert(comp.as_os_str().to_os_string());
            }
        }
    }

    let root_children_count = unique_root_children.len();

    let mut intermediate_counts = BTreeMap::new();
    intermediate_counts.extend(children_map.into_iter().map(|(p, set)| (p, set.len())));

    (root_children_count, intermediate_counts)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        env,
        path::{Path, PathBuf},
    };

    use super::*;

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
            ("foo//bar///baz/", cwd.join("foo").join("bar").join("baz")),
            // Empty and dot paths
            (".", cwd.clone()),
            ("", cwd.clone()),
        ];

        // Platform-specific absolute paths
        if cfg!(windows) {
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

            assert_eq!(
                result, expected,
                "\nNormalization mismatch!\nInput:    {:?}\nExpected: {:?}\nActual:   {:?}",
                input, expected, result
            );
        }

        Ok(())
    }

    #[test]
    fn test_get_absolute_normalized_path_tilde() {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .unwrap_or_default();
        if home.is_empty() {
            return;
        }
        let home_path = PathBuf::from(home);

        let path = Path::new("~/Documents");
        let result = get_absolute_normalized_path(path).unwrap();
        assert_eq!(result, home_path.join("Documents"));

        let path = Path::new("~");
        let result = get_absolute_normalized_path(path).unwrap();
        assert_eq!(result, home_path);

        let path = Path::new("~/..");
        let result = get_absolute_normalized_path(path).unwrap();
        assert_eq!(result, home_path.parent().unwrap_or(&home_path));
    }

    #[test]
    fn test_normalization_is_lexical_only() -> Result<()> {
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

        let paths = vec![PathBuf::from("C:\\a\\b"), PathBuf::from("D:\\x\\y")];
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

        // Test with empty root (virtual root)
        let root = PathBuf::from("");
        let paths = vec![
            PathBuf::from("/a/b"),
            PathBuf::from("/a/c"),
            PathBuf::from("/d"),
        ];
        let (root_children_count, intermediate_paths) = get_intermediate_paths(&root, &paths);
        let mut expected = BTreeMap::new();
        expected.insert(PathBuf::from("/"), 2); // children of /: a and d
        expected.insert(PathBuf::from("/a"), 2); // children of /a: b and c
        assert_eq!(root_children_count, 1); // the root children is just "/"
        assert_eq!(intermediate_paths, expected);
    }

    #[cfg(unix)]
    #[test]
    fn test_abbreviate_path_unix() {
        assert_eq!(abbreviate_path(Path::new("short/path"), 5), "short/path");

        let path = Path::new("/home/user/some/path/to/a/file.txt");
        assert_eq!(abbreviate_path(path, 20), "/home/.../a/file.txt");
        assert_eq!(abbreviate_path(path, 30), "/home/user/some/.../a/file.txt");

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
        assert_eq!(result, r"C:\...\file.txt");
        assert!(!result.contains(r"\\"));

        let result = abbreviate_path(path, 25);
        assert_eq!(result, r"C:\Users\...\file.txt");
        assert!(!result.contains(r"\\"));

        let result = abbreviate_path(path, 30);
        assert_eq!(result, r"C:\Users\Admin\...\file.txt");
        assert!(!result.contains(r"\\"));
    }
}
