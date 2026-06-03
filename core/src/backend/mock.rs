use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use parking_lot::RwLock;

use crate::backend::{BackendNode, Handle, NodeAttr, StorageBackend, WriteContents};

/// Types of operations that can be performed on the backend.
#[derive(Debug, Clone)]
pub enum BackendOp {
    Create,
    PathExists(PathBuf),
    IsFile(PathBuf),
    IsDir(PathBuf),
    Read {
        path: PathBuf,
        offset: isize,
        length: usize,
    },
    Write {
        path: PathBuf,
        length: usize,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
    },
    ListDir(PathBuf),
    CreateDir(PathBuf),
    Remove(PathBuf),
    Lstat(PathBuf),
}

/// The result of a successful backend operation.
#[derive(Debug, Clone)]
pub enum OpResult {
    Unit,
    Bool(bool),
    Bytes(Vec<u8>),
    Nodes(Vec<BackendNode>),
    Attr(NodeAttr),
}

/// A recorded entry in the backend's history.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub op: BackendOp,
    pub result: Result<OpResult, String>, // Store error as String for easy cloning
    pub timestamp: Instant,
}

/// The effect a hook can have on an operation.
#[derive(Default)]
pub struct MockEffect {
    pub delay: Option<Duration>,
    pub result_override: Option<Result<OpResult>>,
}

/// A hook that can intercept and modify backend behavior.
pub type MockHook = Arc<dyn Fn(&BackendOp) -> MockEffect + Send + Sync>;

/// Metadata shared by all node types.
#[derive(Debug, Clone)]
pub struct NodeMeta {
    pub perm: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: SystemTime,
    pub atime: SystemTime,
}

impl NodeMeta {
    fn new(perm: u32) -> Self {
        Self {
            perm,
            uid: 0,
            gid: 0,
            mtime: SystemTime::now(),
            atime: SystemTime::now(),
        }
    }
}

/// A node in the mock filesystem.
#[derive(Debug, Clone)]
pub enum MockNode {
    File { data: Vec<u8>, meta: NodeMeta },
    Dir { meta: NodeMeta },
    Symlink { target: Vec<u8>, meta: NodeMeta },
}

impl MockNode {
    pub fn file(data: Vec<u8>) -> Self {
        Self::File {
            data,
            meta: NodeMeta::new(0o100600),
        }
    }

    pub fn dir() -> Self {
        Self::Dir {
            meta: NodeMeta::new(0o040755),
        }
    }

    pub fn symlink(target: impl Into<String>) -> Self {
        Self::Symlink {
            target: target.into().into_bytes(),
            meta: NodeMeta::new(0o120777),
        }
    }
}

/// A storage backend that stores data in memory.
///
/// Paths are repo-relative (no base path prefix). Every path is an explicit
/// node with a type (file, directory, or symlink) and metadata. This backend
/// is primarily intended for testing.
pub struct MockBackend {
    nodes: Arc<RwLock<BTreeMap<PathBuf, MockNode>>>,
    history: Arc<RwLock<Vec<HistoryEntry>>>,
    hooks: Arc<RwLock<Vec<MockHook>>>,
}

impl MockBackend {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            history: Arc::new(RwLock::new(Vec::new())),
            hooks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Adds a hook to customize backend behavior.
    pub fn add_hook(&self, hook: MockHook) {
        self.hooks.write().push(hook);
    }

    /// Clears all active hooks.
    pub fn clear_hooks(&self) {
        self.hooks.write().clear();
    }

    /// Returns a copy of the operation history.
    pub fn history(&self) -> Vec<HistoryEntry> {
        self.history.read().clone()
    }

    /// Clears the operation history.
    pub fn clear_history(&self) {
        self.history.write().clear();
    }

    /// Internal helper to execute an operation with instrumentation and hooks.
    async fn exec<F, Fut>(&self, op: BackendOp, f: F) -> Result<OpResult>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<OpResult>>,
    {
        let mut total_delay = Duration::ZERO;
        let mut result_override = None;

        // Apply hooks
        {
            let hooks = self.hooks.read();
            for hook in hooks.iter() {
                let effect = hook(&op);
                if let Some(d) = effect.delay {
                    total_delay += d;
                }
                if let Some(res) = effect.result_override {
                    result_override = Some(res);
                }
            }
        }

        if !total_delay.is_zero() {
            tokio::time::sleep(total_delay).await;
        }

        let result = if let Some(res) = result_override {
            res
        } else {
            f().await
        };

        // Record history
        {
            let entry = HistoryEntry {
                op,
                result: result
                    .as_ref()
                    .map(|r| r.clone())
                    .map_err(|e| e.to_string()),
                timestamp: Instant::now(),
            };
            self.history.write().push(entry);
        }

        result
    }

    /// Ensures all parent directories of `path` exist as MockNode::Dir,
    /// matching LocalFS behavior where write/create_dir create parent dirs implicitly.
    fn ensure_parent_dirs(nodes: &mut BTreeMap<PathBuf, MockNode>, path: &Path) {
        let mut p = path.parent();
        while let Some(parent) = p {
            if parent.as_os_str().is_empty() {
                break;
            }
            nodes
                .entry(parent.to_path_buf())
                .or_insert_with(MockNode::dir);
            p = parent.parent();
        }
    }

    /// Inserts a file node with default metadata.
    pub fn put_file(&self, path: impl AsRef<Path>, data: Vec<u8>) {
        self.nodes
            .write()
            .insert(path.as_ref().to_path_buf(), MockNode::file(data));
    }

    /// Inserts a directory node with default metadata.
    pub fn put_dir(&self, path: impl AsRef<Path>) {
        self.nodes
            .write()
            .insert(path.as_ref().to_path_buf(), MockNode::dir());
    }

    /// Inserts a symlink node with default metadata.
    pub fn put_symlink(&self, path: impl AsRef<Path>, target: impl Into<String>) {
        self.nodes
            .write()
            .insert(path.as_ref().to_path_buf(), MockNode::symlink(target));
    }

    /// Retrieves file contents or symlink target, if the path exists.
    pub fn get_file(&self, path: impl AsRef<Path>) -> Option<Vec<u8>> {
        self.nodes.read().get(path.as_ref()).and_then(|n| match n {
            MockNode::File { data, .. } => Some(data.clone()),
            MockNode::Symlink { target, .. } => Some(target.clone()),
            MockNode::Dir { .. } => None,
        })
    }

    /// Returns a reference to the internal node map for inspection.
    pub fn nodes(&self) -> Arc<RwLock<BTreeMap<PathBuf, MockNode>>> {
        self.nodes.clone()
    }

    /// Removes all nodes from the store.
    pub fn clear(&self) {
        self.nodes.write().clear();
    }
}

impl Default for MockBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for MockBackend {
    async fn create(&self) -> Result<()> {
        self.exec(BackendOp::Create, || async { Ok(OpResult::Unit) })
            .await?;
        Ok(())
    }

    async fn path_exists(&self, path: &Path) -> bool {
        let res = self
            .exec(BackendOp::PathExists(path.to_path_buf()), || async {
                Ok(OpResult::Bool(self.nodes.read().contains_key(path)))
            })
            .await;
        match res {
            Ok(OpResult::Bool(b)) => b,
            _ => false,
        }
    }

    async fn is_file(&self, path: &Path) -> bool {
        let res = self
            .exec(BackendOp::IsFile(path.to_path_buf()), || async {
                Ok(OpResult::Bool(
                    self.nodes
                        .read()
                        .get(path)
                        .is_some_and(|n| matches!(n, MockNode::File { .. })),
                ))
            })
            .await;
        match res {
            Ok(OpResult::Bool(b)) => b,
            _ => false,
        }
    }

    async fn is_dir(&self, path: &Path) -> bool {
        let res = self
            .exec(BackendOp::IsDir(path.to_path_buf()), || async {
                Ok(OpResult::Bool(
                    self.nodes
                        .read()
                        .get(path)
                        .is_some_and(|n| matches!(n, MockNode::Dir { .. })),
                ))
            })
            .await;
        match res {
            Ok(OpResult::Bool(b)) => b,
            _ => false,
        }
    }

    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let op = BackendOp::Read {
            path: handle.path.to_path_buf(),
            offset,
            length,
        };
        let res = self
            .exec(op, || async {
                let nodes = self.nodes.read();
                let node = nodes.get(handle.path).with_context(|| {
                    format!("MockBackend: path not found: {}", handle.path.display())
                })?;

                let file_size = match node {
                    MockNode::File { data, .. } => data.len(),
                    _ => {
                        return Err(anyhow!(
                            "MockBackend: cannot read — not a file: {}",
                            handle.path.display()
                        ));
                    }
                };

                let start: usize = if offset >= 0 {
                    offset as usize
                } else {
                    file_size.saturating_sub(offset.unsigned_abs())
                };

                if start >= file_size {
                    return Ok(OpResult::Bytes(Vec::new()));
                }

                let end = if length == 0 {
                    file_size
                } else {
                    start.saturating_add(length).min(file_size)
                };

                match node {
                    MockNode::File { data, .. } => Ok(OpResult::Bytes(data[start..end].to_vec())),
                    _ => unreachable!(),
                }
            })
            .await?;

        match res {
            OpResult::Bytes(b) => Ok(b),
            _ => unreachable!(),
        }
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        let op = BackendOp::Write {
            path: handle.path.to_path_buf(),
            length: contents.len(),
        };
        self.exec(op, || async {
            let mut nodes = self.nodes.write();

            if let Some(n) = nodes.get(handle.path)
                && matches!(n, MockNode::Dir { .. })
            {
                return Err(anyhow!(
                    "MockBackend: cannot write — is a directory: {}",
                    handle.path.display()
                ));
            }

            Self::ensure_parent_dirs(&mut nodes, handle.path);
            nodes.insert(
                handle.path.to_path_buf(),
                MockNode::file(contents.into_owned()),
            );
            Ok(OpResult::Unit)
        })
        .await?;
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let op = BackendOp::Rename {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
        };
        self.exec(op, || async {
            let mut nodes = self.nodes.write();

            Self::ensure_parent_dirs(&mut nodes, to);

            let keys: Vec<PathBuf> = nodes
                .keys()
                .filter(|k| *k == from || k.starts_with(from))
                .cloned()
                .collect();

            if keys.is_empty() {
                return Err(anyhow!(
                    "MockBackend: cannot rename — source not found: {from:?}"
                ));
            }

            for k in &keys {
                let node = nodes.remove(k).unwrap();
                let new_key = if *k == from {
                    to.to_path_buf()
                } else {
                    let rel = k.strip_prefix(from).unwrap();
                    to.join(rel)
                };
                nodes.insert(new_key, node);
            }

            Ok(OpResult::Unit)
        })
        .await?;
        Ok(())
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        let op = BackendOp::CreateDir(path.to_path_buf());
        self.exec(op, || async {
            let mut nodes = self.nodes.write();
            Self::ensure_parent_dirs(&mut nodes, path);
            nodes
                .entry(path.to_path_buf())
                .or_insert_with(MockNode::dir);
            Ok(OpResult::Unit)
        })
        .await?;
        Ok(())
    }

    async fn remove(&self, path: &Path) -> Result<()> {
        let op = BackendOp::Remove(path.to_path_buf());
        self.exec(op, || async {
            let mut nodes = self.nodes.write();

            let prefix = PathBuf::from(path);
            let to_remove: Vec<PathBuf> = nodes
                .keys()
                .filter(|p| *p == &prefix || p.starts_with(&prefix))
                .cloned()
                .collect();

            if to_remove.is_empty() {
                return Err(anyhow!(
                    "MockBackend: cannot remove — path not found: {}",
                    path.display()
                ));
            }

            for p in &to_remove {
                nodes.remove(p);
            }

            Ok(OpResult::Unit)
        })
        .await?;
        Ok(())
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<BackendNode>> {
        let op = BackendOp::ListDir(path.to_path_buf());
        let res = self
            .exec(op, || async {
                let mut result: Vec<BackendNode> = Vec::new();
                let prefix = if path == Path::new("") {
                    PathBuf::new()
                } else {
                    PathBuf::from(path)
                };

                for (key, node) in self.nodes.read().iter() {
                    if !key.starts_with(&prefix) || key == &prefix {
                        continue;
                    }

                    let rel = key.strip_prefix(&prefix).unwrap_or(key);
                    let first = match rel.components().next() {
                        Some(c) => PathBuf::from(c.as_os_str()),
                        None => continue,
                    };

                    let node_path = if prefix.as_os_str().is_empty() {
                        first
                    } else {
                        prefix.join(&first)
                    };

                    if result.iter().any(|n| n.path() == node_path) {
                        continue;
                    }

                    if rel.components().count() > 1 || matches!(node, MockNode::Dir { .. }) {
                        result.push(BackendNode::Dir(node_path));
                    } else {
                        let len = match node {
                            MockNode::File { data, .. } => data.len() as u64,
                            _ => 0,
                        };
                        result.push(BackendNode::File(node_path, len));
                    }
                }

                Ok(OpResult::Nodes(result))
            })
            .await?;

        match res {
            OpResult::Nodes(n) => Ok(n),
            _ => unreachable!(),
        }
    }

    async fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        let op = BackendOp::Lstat(path.to_path_buf());
        let res = self
            .exec(op, || async {
                let node = self.nodes.read().get(path).cloned().with_context(|| {
                    format!("MockBackend: lstat — path not found: {}", path.display())
                })?;

                let (size, meta) = match &node {
                    MockNode::File { data, meta } => (Some(data.len() as u64), meta),
                    MockNode::Dir { meta } => (Some(0), meta),
                    MockNode::Symlink { target, meta } => (Some(target.len() as u64), meta),
                };

                Ok(OpResult::Attr(NodeAttr {
                    size,
                    uid: Some(meta.uid),
                    gid: Some(meta.gid),
                    perm: Some(meta.perm),
                    atime: Some(meta.atime),
                    mtime: Some(meta.mtime),
                }))
            })
            .await?;
        match res {
            OpResult::Attr(a) => Ok(a),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::Handle;

    use super::*;

    #[tokio::test]
    async fn test_write_and_read_file() {
        let backend = MockBackend::new();
        let handle = Handle::new(Path::new("data/test.txt"));

        backend
            .write(&handle, WriteContents::Borrowed(b"hello world"))
            .await
            .unwrap();

        assert!(backend.path_exists(handle.path).await);
        assert!(backend.is_file(handle.path).await);
        assert!(!backend.is_dir(handle.path).await);

        let data = backend.read(&handle, 0, 0).await.unwrap();
        assert_eq!(data, b"hello world");
    }

    #[tokio::test]
    async fn test_read_with_offset_and_length() {
        let backend = MockBackend::new();
        backend.put_file("data/test.txt", b"0123456789".to_vec());

        let handle = Handle::new(Path::new("data/test.txt"));

        let data = backend.read(&handle, 3, 4).await.unwrap();
        assert_eq!(data, b"3456");

        let data = backend.read(&handle, 100, 4).await.unwrap();
        assert!(data.is_empty());

        let data = backend.read(&handle, -3, 0).await.unwrap();
        assert_eq!(data, b"789");
    }

    #[tokio::test]
    async fn test_read_nonexistent_path() {
        let backend = MockBackend::new();
        let handle = Handle::new(Path::new("no/such/file"));

        let result = backend.read(&handle, 0, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_directory_fails() {
        let backend = MockBackend::new();
        backend.put_dir("dir");

        let handle = Handle::new(Path::new("dir"));
        let result = backend.read(&handle, 0, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_overwrites_file() {
        let backend = MockBackend::new();
        let handle = Handle::new(Path::new("f.txt"));

        backend
            .write(&handle, WriteContents::Borrowed(b"old"))
            .await
            .unwrap();
        backend
            .write(&handle, WriteContents::Borrowed(b"new"))
            .await
            .unwrap();

        let data = backend.read(&handle, 0, 0).await.unwrap();
        assert_eq!(data, b"new");
    }

    #[tokio::test]
    async fn test_write_over_directory_fails() {
        let backend = MockBackend::new();
        backend.put_dir("dir");

        let handle = Handle::new(Path::new("dir"));
        let result = backend
            .write(&handle, WriteContents::Borrowed(b"data"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rename_file() {
        let backend = MockBackend::new();
        backend.put_file("old.txt", b"data".to_vec());

        backend
            .rename(Path::new("old.txt"), Path::new("new.txt"))
            .await
            .unwrap();

        assert!(!backend.path_exists(Path::new("old.txt")).await);
        assert!(backend.path_exists(Path::new("new.txt")).await);
        assert!(backend.is_file(Path::new("new.txt")).await);
    }

    #[tokio::test]
    async fn test_rename_directory() {
        let backend = MockBackend::new();
        backend.put_dir("old");
        backend.put_file("old/a.txt", b"x".to_vec());

        backend
            .rename(Path::new("old"), Path::new("new"))
            .await
            .unwrap();

        assert!(backend.is_dir(Path::new("new")).await);
        assert!(backend.is_file(Path::new("new/a.txt")).await);
    }

    #[tokio::test]
    async fn test_rename_nonexistent_source() {
        let backend = MockBackend::new();
        let result = backend.rename(Path::new("missing"), Path::new("new")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_remove_file() {
        let backend = MockBackend::new();
        backend.put_file("f.txt", b"data".to_vec());

        backend.remove(Path::new("f.txt")).await.unwrap();
        assert!(!backend.path_exists(Path::new("f.txt")).await);
    }

    #[tokio::test]
    async fn test_remove_dir() {
        let backend = MockBackend::new();
        backend.put_dir("dir");

        backend.remove(Path::new("dir")).await.unwrap();
        assert!(!backend.path_exists(Path::new("dir")).await);
    }

    #[tokio::test]
    async fn test_remove_dir_recursive() {
        let backend = MockBackend::new();
        backend.put_dir("dir");
        backend.put_file("dir/a.txt", b"a".to_vec());
        backend.put_file("dir/b.txt", b"b".to_vec());
        backend.put_dir("dir/sub");
        backend.put_file("dir/sub/c.txt", b"c".to_vec());

        backend.remove(Path::new("dir")).await.unwrap();

        assert!(!backend.path_exists(Path::new("dir")).await);
        assert!(!backend.path_exists(Path::new("dir/a.txt")).await);
        assert!(!backend.path_exists(Path::new("dir/sub/c.txt")).await);
    }

    #[tokio::test]
    async fn test_remove_nonexistent() {
        let backend = MockBackend::new();
        let result = backend.remove(Path::new("no/such/file")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_dir_flat() {
        let backend = MockBackend::new();
        backend.put_file("a.txt", b"1".to_vec());
        backend.put_file("b.txt", b"2".to_vec());
        backend.put_dir("sub");
        backend.put_file("sub/c.txt", b"3".to_vec());

        let mut nodes = backend.list_dir(Path::new("")).await.unwrap();
        nodes.sort_by(|a, b| a.path().cmp(b.path()));

        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].path(), Path::new("a.txt"));
        assert_eq!(nodes[1].path(), Path::new("b.txt"));
        assert!(matches!(nodes[2], BackendNode::Dir(_)));
        assert_eq!(nodes[2].path(), Path::new("sub"));
    }

    #[tokio::test]
    async fn test_list_dir_nested() {
        let backend = MockBackend::new();
        backend.put_dir("sub");
        backend.put_file("sub/c.txt", b"x".to_vec());
        backend.put_dir("sub/deep");
        backend.put_file("sub/deep/d.txt", b"y".to_vec());

        let mut nodes = backend.list_dir(Path::new("sub")).await.unwrap();
        nodes.sort_by(|a, b| a.path().cmp(b.path()));

        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].path(), Path::new("sub/c.txt"));
        assert!(matches!(nodes[1], BackendNode::Dir(_)));
        assert_eq!(nodes[1].path(), Path::new("sub/deep"));
    }

    #[tokio::test]
    async fn test_list_dir_inside_file() {
        let backend = MockBackend::new();
        backend.put_file("f.txt", b"x".to_vec());

        let nodes = backend.list_dir(Path::new("f.txt")).await.unwrap();
        assert!(nodes.is_empty());
    }

    #[tokio::test]
    async fn test_list_dir_empty() {
        let backend = MockBackend::new();
        let nodes = backend.list_dir(Path::new("")).await.unwrap();
        assert!(nodes.is_empty());
    }

    #[tokio::test]
    async fn test_create_dir_and_is_dir() {
        let backend = MockBackend::new();

        backend.create_dir(Path::new("a/b/c")).await.unwrap();

        assert!(backend.path_exists(Path::new("a/b/c")).await);
        assert!(backend.is_dir(Path::new("a/b/c")).await);
        assert!(!backend.is_file(Path::new("a/b/c")).await);
    }

    #[tokio::test]
    async fn test_is_dir_for_file() {
        let backend = MockBackend::new();
        backend.put_file("f.txt", b"x".to_vec());

        assert!(!backend.is_dir(Path::new("f.txt")).await);
        assert!(backend.is_file(Path::new("f.txt")).await);
    }

    #[tokio::test]
    async fn test_path_exists_nonexistent() {
        let backend = MockBackend::new();
        assert!(!backend.path_exists(Path::new("nothing")).await);
    }

    #[tokio::test]
    async fn test_lstat_file() {
        let backend = MockBackend::new();
        backend.put_file("f.txt", b"hello".to_vec());

        let attr = backend.lstat(Path::new("f.txt")).await.unwrap();
        assert_eq!(attr.size, Some(5));
        assert!(attr.perm.is_some());
        assert!(attr.uid.is_some());
        assert!(attr.gid.is_some());
        assert!(attr.mtime.is_some());
    }

    #[tokio::test]
    async fn test_lstat_dir() {
        let backend = MockBackend::new();
        backend.put_dir("d");

        let attr = backend.lstat(Path::new("d")).await.unwrap();
        assert_eq!(attr.size, Some(0));
        assert!(attr.perm.is_some());
    }

    #[tokio::test]
    async fn test_lstat_nonexistent() {
        let backend = MockBackend::new();
        let result = backend.lstat(Path::new("missing")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_symlink() {
        let backend = MockBackend::new();
        backend.put_symlink("link", "/target/path");

        assert!(backend.path_exists(Path::new("link")).await);
        assert!(!backend.is_file(Path::new("link")).await);
        assert!(!backend.is_dir(Path::new("link")).await);

        let node = backend
            .nodes
            .read()
            .get(Path::new("link"))
            .cloned()
            .unwrap();
        assert!(matches!(node, MockNode::Symlink { .. }));
        assert_eq!(
            match node {
                MockNode::Symlink { ref target, .. } => target.clone(),
                _ => unreachable!(),
            },
            b"/target/path"
        );
    }

    #[tokio::test]
    async fn test_create_dir_is_idempotent() {
        let backend = MockBackend::new();
        backend.create_dir(Path::new("dir")).await.unwrap();
        backend.create_dir(Path::new("dir")).await.unwrap();

        assert!(backend.is_dir(Path::new("dir")).await);
    }

    #[tokio::test]
    async fn test_clear() {
        let backend = MockBackend::new();
        backend.put_file("a.txt", b"1".to_vec());
        backend.put_dir("d");

        backend.clear();

        assert!(!backend.path_exists(Path::new("a.txt")).await);
        assert!(!backend.path_exists(Path::new("d")).await);
    }

    #[tokio::test]
    async fn test_is_dry_run() {
        let backend = MockBackend::new();
        assert!(!backend.is_dry_run());
    }

    #[tokio::test]
    async fn test_history_recording() {
        let backend = MockBackend::new();
        let handle = Handle::new(Path::new("f.txt"));

        backend
            .write(&handle, WriteContents::Borrowed(b"data"))
            .await
            .unwrap();
        backend.path_exists(handle.path).await;
        backend.read(&handle, 0, 0).await.unwrap();

        let history = backend.history();
        assert_eq!(history.len(), 3);

        assert!(matches!(history[0].op, BackendOp::Write { .. }));
        assert!(matches!(history[1].op, BackendOp::PathExists(_)));
        assert!(matches!(history[2].op, BackendOp::Read { .. }));

        if let BackendOp::Write { path, length } = &history[0].op {
            assert_eq!(path, Path::new("f.txt"));
            assert_eq!(*length, 4);
        }
    }

    #[tokio::test]
    async fn test_delay_hook() {
        const DELAY_MS: u64 = 100;

        let backend = MockBackend::new();
        backend.add_hook(Arc::new(|_| MockEffect {
            delay: Some(Duration::from_millis(DELAY_MS)),
            ..Default::default()
        }));

        let start = Instant::now();
        backend.path_exists(Path::new("any")).await;
        assert!(start.elapsed() >= Duration::from_millis(DELAY_MS));
    }

    #[tokio::test]
    async fn test_error_injection_hook() {
        let backend = MockBackend::new();
        backend.add_hook(Arc::new(|op| {
            if let BackendOp::Read { path, .. } = op
                && path == Path::new("fail_me")
            {
                return MockEffect {
                    result_override: Some(Err(anyhow!("injected failure"))),
                    ..Default::default()
                };
            }
            MockEffect::default()
        }));

        backend.put_file("fail_me", b"data".to_vec());
        backend.put_file("ok_me", b"data".to_vec());

        let res = backend.read(&Handle::new(Path::new("fail_me")), 0, 0).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "injected failure");

        let res = backend.read(&Handle::new(Path::new("ok_me")), 0, 0).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_result_override_hook() {
        let backend = MockBackend::new();
        backend.add_hook(Arc::new(|op| {
            if let BackendOp::PathExists(p) = op
                && p == Path::new("lying_path")
            {
                return MockEffect {
                    result_override: Some(Ok(OpResult::Bool(true))),
                    ..Default::default()
                };
            }
            MockEffect::default()
        }));

        // Path does not exist in nodes, but hook says it does
        assert!(backend.path_exists(Path::new("lying_path")).await);
        assert!(!backend.path_exists(Path::new("other")).await);
    }
}
