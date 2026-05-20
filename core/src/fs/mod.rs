use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
};

use anyhow::Result;

pub mod filter;
pub mod node;

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
    // 1. Expand tilde if present.
    let path_buf = expand_tilde(path);
    let path_ref = path_buf.as_path();

    // 2. On Windows, convert MSYS2 paths.
    #[cfg(windows)]
    let msys_path;
    #[cfg(windows)]
    let mut path_ref = path_ref;
    #[cfg(windows)]
    if let Some(p) = convert_msys_path(path_ref) {
        msys_path = p;
        path_ref = msys_path.as_path();
    }

    // 3. Make absolute by joining with CWD if relative.
    let absolute_path = if path_ref.is_absolute() {
        path_ref.to_path_buf()
    } else {
        std::env::current_dir()?.join(path_ref)
    };

    // 4. Lexically normalize components (resolve . and ..).
    let mut components = Vec::new();
    for component in absolute_path.components() {
        match component {
            #[allow(unused_variables)]
            Component::Prefix(prefix) => {
                #[cfg(windows)]
                components.push(Component::Prefix(prefix));
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
    let last = components.last().unwrap().as_os_str().to_string_lossy();

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

    loop {
        let sep = if cfg!(windows) { "\\" } else { "/" };
        let next_left = if left_idx <= right_idx {
            let s = components[left_idx].as_os_str().to_string_lossy();
            format!("{}{}{}", result_left, sep, s)
        } else {
            result_left.clone()
        };

        let next_right = if right_idx >= left_idx {
            let s = components[right_idx].as_os_str().to_string_lossy();
            format!("{}{}{}", s, sep, result_right)
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

    let sep = if cfg!(windows) { "\\" } else { "/" };
    format!("{}{}{}{}{}", result_left, sep, "...", sep, result_right)
}

/// Helper function to build intermediate path maps for streaming.
pub fn get_intermediate_paths(root: &Path, paths: &[PathBuf]) -> (usize, BTreeMap<PathBuf, usize>) {
    let mut map = BTreeMap::new();
    let mut root_children = 0;

    for path in paths {
        if let Ok(relative) = path.strip_prefix(root) {
            let components: Vec<_> = relative.components().collect();
            if components.is_empty() {
                continue;
            }

            root_children += 1; // Direct child of root

            let mut current = root.to_path_buf();
            for component in components.iter().take(components.len().saturating_sub(1)) {
                current.push(component);
                // Don't add the root directory itself to the intermediate map
                if matches!(component, Component::RootDir) {
                    continue;
                }
                *map.entry(current.clone()).or_insert(0) += 1;
            }
        }
    }

    (root_children, map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_absolute_normalized_path() {
        let cwd = std::env::current_dir().unwrap();

        // Relative path
        let path = Path::new("some/path");
        assert_eq!(
            get_absolute_normalized_path(path).unwrap(),
            cwd.join("some/path")
        );

        // Path with .
        let path = Path::new("./some/./path");
        assert_eq!(
            get_absolute_normalized_path(path).unwrap(),
            cwd.join("some/path")
        );

        // Path with ..
        let path = Path::new("some/other/../path");
        assert_eq!(
            get_absolute_normalized_path(path).unwrap(),
            cwd.join("some/path")
        );

        // Absolute path
        #[cfg(unix)]
        {
            let path = Path::new("/absolute/path");
            assert_eq!(
                get_absolute_normalized_path(path).unwrap(),
                PathBuf::from("/absolute/path")
            );
        }

        #[cfg(windows)]
        {
            let path = Path::new(r"C:\absolute\path");
            assert_eq!(
                get_absolute_normalized_path(path).unwrap(),
                PathBuf::from(r"C:\absolute\path")
            );
        }
    }

    #[test]
    fn test_calculate_lcp() {
        // Multiple paths
        let paths = vec![
            PathBuf::from("/home/user/project/file1.txt"),
            PathBuf::from("/home/user/project/subdir/file2.txt"),
            PathBuf::from("/home/user/project/file3.txt"),
        ];
        assert_eq!(
            calculate_lcp(&paths, false),
            PathBuf::from("/home/user/project")
        );

        // Single path
        let paths = vec![PathBuf::from("/home/user/file.txt")];
        assert_eq!(calculate_lcp(&paths, false), PathBuf::from("/home/user"));
        assert_eq!(
            calculate_lcp(&paths, true),
            PathBuf::from("/home/user/file.txt")
        );

        // No common prefix
        let paths = vec![PathBuf::from("/a/b"), PathBuf::from("/c/d")];
        #[cfg(unix)]
        assert_eq!(calculate_lcp(&paths, false), PathBuf::from("/"));
    }

    #[test]
    fn test_get_intermediate_paths() {
        let root = PathBuf::from("/a");
        let paths = vec![
            PathBuf::from("/a/b/c"),
            PathBuf::from("/a/b/d"),
            PathBuf::from("/a/e"),
        ];

        let (root_children_count, intermediate_paths) = get_intermediate_paths(&root, &paths);

        let mut expected = BTreeMap::new();
        expected.insert(PathBuf::from("/a/b"), 2);
        assert_eq!(root_children_count, 3); // 'b/c', 'b/d', and 'e' are children
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
        expected.insert(PathBuf::from("/a"), 2); // children of /a: b and c
        assert_eq!(root_children_count, 3); // children of virtual root are /a and /d
        assert_eq!(intermediate_paths, expected);
    }

    #[cfg(unix)]
    #[test]
    fn test_abbreviate_path_unix() {
        // Note: On Unix, the first component of "/home/user" is "/"

        assert_eq!(abbreviate_path(Path::new("short/path"), 5), "short/path");

        let path = Path::new("/home/user/some/path/to/a/file.txt");
        // max_len=20: "/home/.../a/file.txt" (20 chars)
        assert_eq!(abbreviate_path(path, 20), "/home/.../a/file.txt");
        // max_len=30: "/home/user/some/.../a/file.txt" (30 chars)
        assert_eq!(abbreviate_path(path, 30), "/home/user/some/.../a/file.txt");

        let long_path_str = "/home/user/projects/backup_tool/src/core/permissions.rs";
        let long_path = Path::new(long_path_str);

        // max_len=30: "/home/user/.../permissions.rs"
        assert_eq!(
            abbreviate_path(long_path, 30),
            "/home/user/.../permissions.rs"
        );
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
}
