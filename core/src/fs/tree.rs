use std::{
    cmp::Ordering,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::{
    fs::{self, node::Node},
    mapache::{BlobType, ID, SaveID},
    repository::repo::{Repository, SizePair},
    utils,
};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Tree {
    pub nodes: Vec<Node>,
}

impl Tree {
    /// Creates a new empty tree.
    pub fn new(nodes: Vec<Node>) -> Self {
        Self { nodes }
    }

    /// Saves a tree in the repository. This function should be called when a tree is complete,
    /// that is, when all the contents and/or tree hashes have been resolved.
    pub fn save_to_repo(&mut self, repo: &Repository) -> Result<(ID, SizePair)> {
        // Sort all nodes by name before serializing
        self.nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        // Reserve some space for each node's text. This value is a heuristic.
        let mut buffer = Vec::with_capacity(self.nodes.len() * 160);
        serde_json::to_writer(&mut buffer, self).context("Failed to serialize tree nodes")?;

        let mut encoding_context = repo.get_encoding_context()?;
        let (id, data_size, meta_size) = repo.encode_and_save_blob(
            &mut encoding_context,
            BlobType::Tree,
            buffer,
            SaveID::CalculateID,
        )?;

        let total_size = data_size + meta_size;

        Ok((id, total_size))
    }

    /// Load a tree from the repository.
    pub fn load_from_repo(repo: &Repository, root_id: &ID) -> Result<Tree> {
        let tree_object = repo.load_blob(root_id)?;
        let tree: Tree = serde_json::from_slice(&tree_object)?;
        Ok(tree)
    }
}

/// Represents a file system node along with additional information needed for streaming.
/// This structure is used by the various streaming iterators.
#[derive(Debug)]
pub struct StreamNode {
    pub node: Node,
    /// The number of children this node has that will be yielded by the stream.
    /// This is 0 for files or symlinks.
    pub num_children: usize,
}

/// A tuple representing an item yielded by the node streams:
/// (full path of the node, the stream node itself).
pub type StreamNodeInfo = (PathBuf, StreamNode);

/// A depth‑first *pre‑order* filesystem stream.
///
/// Items are produced in lexicographical order of their *full* paths. The root path is not emitted.
/// The internal stack only stores the nodes strictly necessary for iteration. The full tree is not
/// stored in memory. The iteration with a stack avoids recursive calls.
///
/// This stream will emit all the merged nodes as if they belong to the same tree,
/// intercalating intermediate paths between disjoint branches.
/// This stream also allows excluding a list of paths. Paths in this list, and their
/// children, are never explored nor emitted.
#[derive(Debug)]
pub struct FSNodeStream {
    stack: Vec<PathBuf>,
    intermediate_paths: Vec<(PathBuf, usize)>,
    exclude_paths: Vec<PathBuf>,
}

impl FSNodeStream {
    /// Creates an FSNodeStream from multiple root paths. The paths are iterated in lexicographical order.
    /// Exclude paths and their children are neither emitted nor explored into.
    pub fn from_paths(mut paths: Vec<PathBuf>, mut exclude_paths: Vec<PathBuf>) -> Result<Self> {
        for path in &paths {
            if !fs::path_exists(path) {
                bail!("Path {} does not exist", path.display());
            }
        }

        exclude_paths.sort_unstable();
        paths.retain(|path| utils::filter_path(path, None, Some(exclude_paths.as_ref())));

        // Calculate intermediate paths and count children (root included)
        let common_root = utils::calculate_lcp(&paths, false);
        let (_root_children_count, intermediate_path_set) =
            utils::get_intermediate_paths(&common_root, &paths);

        // Filter intermediate paths based on exclude_paths and collect
        let mut intermediate_paths: Vec<(PathBuf, usize)> = intermediate_path_set
            .into_iter()
            .filter(|(path, _)| utils::filter_path(path, None, Some(exclude_paths.as_ref())))
            .collect();

        // Sort paths in reverse order
        paths.sort_unstable_by(|first, second| second.cmp(first));
        intermediate_paths.sort_unstable_by(|(first, _), (second, _)| second.cmp(first));

        Ok(Self {
            stack: paths,
            intermediate_paths,
            exclude_paths,
        })
    }

    // Get all children sorted in lexicographical order.
    fn get_children_sorted(dir: &Path) -> Result<Vec<PathBuf>> {
        let read_dir = std::fs::read_dir(dir).map_err(|e| anyhow!("Cannot read {dir:?}: {e}"))?;
        let mut entries: Vec<_> = read_dir.collect::<Result<Vec<_>, std::io::Error>>()?;
        entries.sort_unstable_by_key(|a| a.file_name());
        Ok(entries.into_iter().map(|e| e.path()).collect())
    }
}

impl Iterator for FSNodeStream {
    type Item = Result<StreamNodeInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        // Helper to peek the next path in each list
        fn peek_path(entry: &(PathBuf, usize)) -> &PathBuf {
            &entry.0
        }

        // Decide which source has the lexicographically smaller “next” element
        let take_intermediate = loop {
            match (self.intermediate_paths.last(), self.stack.last()) {
                (Some(iv), Some(sv)) => {
                    let iv_path = peek_path(iv);
                    let sv_path = sv;

                    // Skip intermediate if it's excluded
                    if !utils::filter_path(iv_path, None, Some(&self.exclude_paths)) {
                        self.intermediate_paths.pop();
                        continue;
                    }
                    // Skip stack path if it's excluded
                    if !utils::filter_path(sv_path, None, Some(&self.exclude_paths)) {
                        self.stack.pop();
                        continue;
                    }

                    break iv_path.cmp(sv_path) == std::cmp::Ordering::Less;
                }
                (Some(iv), None) => {
                    let iv_path = peek_path(iv);
                    if !utils::filter_path(iv_path, None, Some(&self.exclude_paths)) {
                        self.intermediate_paths.pop();
                        continue;
                    }
                    break true;
                }
                (None, Some(sv)) => {
                    if !utils::filter_path(sv, None, Some(&self.exclude_paths)) {
                        self.stack.pop();
                        continue;
                    }
                    break false;
                }
                (None, None) => return None, // Both are empty
            }
        };

        if take_intermediate {
            let (path, num_children) = self.intermediate_paths.pop().unwrap();
            let node = match Node::from_path(&path) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };

            return Some(Ok((path.clone(), StreamNode { node, num_children })));
        }

        // Otherwise pop from the DFS stack as before
        let path = self.stack.pop().unwrap(); // We know it's not None due to the loop logic
        let result = (|| {
            let node = Node::from_path(&path)?;

            let num_children = if node.is_dir() {
                let children = Self::get_children_sorted(&path)?;
                let mut valid_children_count = 0;

                for child in children.into_iter().rev() {
                    if utils::filter_path(&child, None, Some(&self.exclude_paths)) {
                        self.stack.push(child);
                        valid_children_count += 1;
                    }
                }
                valid_children_count
            } else {
                0
            };

            Ok((path, StreamNode { node, num_children }))
        })();

        Some(result)
    }
}

/// A depth‑first *pre‑order* stream of serialized nodes.
///
/// Items are produced in lexicographical order of their *full* paths. The root node is not emitted.
/// Trees are loaded from the repository as they are needed. The full tree is not stored in memory.
/// The iteration with a stack avoids recursive calls.
///
/// This stream also allows including and excluding a list of paths. Paths in the exclude list, and their
/// children, are never explored nor emitted. If the include list is not empty, only nodes in the same branch
/// (children and parents (intermediate nodes to reach the included path)) as those paths will be emitted.
pub struct SerializedNodeStream {
    repo: Arc<Repository>,
    stack: Vec<StreamNodeInfo>,
    include: Option<Vec<PathBuf>>,
    exclude: Option<Vec<PathBuf>>,
}

impl SerializedNodeStream {
    pub fn new(
        repo: Arc<Repository>,
        root_id: Option<ID>,
        base_path: PathBuf,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
    ) -> Result<Self> {
        let mut stack = Vec::new();

        if let Some(id) = root_id {
            let mut tree = Tree::load_from_repo(repo.as_ref(), &id)
                .with_context(|| format!("Failed to load root tree with ID {id}"))?;

            tree.nodes
                .sort_unstable_by(|first, second| first.name.cmp(&second.name));
            for node in tree.nodes.into_iter().rev() {
                stack.push((
                    base_path.clone(),
                    StreamNode {
                        node,

                        // Actual child count will be determined when this node is processed by `next`.
                        // Initialize to 0 for consistency with how FSNodeStream initializes non-directories.
                        num_children: 0,
                    },
                ));
            }
        }

        Ok(Self {
            repo,
            stack,
            include,
            exclude,
        })
    }
}

impl Iterator for SerializedNodeStream {
    type Item = Result<StreamNodeInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        let (current_path, mut stream_node) = loop {
            let (cpath, node) = match self.stack.pop() {
                None => return None,
                Some((parent_path, stream_node)) => {
                    let current_path = parent_path.join(&stream_node.node.name);
                    (current_path, stream_node)
                }
            };

            if utils::filter_path(&cpath, self.include.as_ref(), self.exclude.as_ref()) {
                break (cpath, node);
            }
        };

        let res = (|| {
            // If it’s a subtree (i.e., a directory), load its children and push them.
            // Also, update the current `stream_node`'s `num_children` with its actual count.
            if let Some(subtree_id) = &stream_node.node.tree {
                let subtree = Tree::load_from_repo(self.repo.as_ref(), subtree_id)?;

                // Filter children based on include/exclude lists before counting and pushing.
                let mut filtered_children = Vec::new();
                for subnode in subtree.nodes.into_iter() {
                    let child_path = current_path.join(&subnode.name);
                    if utils::filter_path(&child_path, self.include.as_ref(), self.exclude.as_ref())
                    {
                        filtered_children.push(subnode);
                    }
                }

                stream_node.num_children = filtered_children.len();

                // Push filtered children for the next iteration, in reverse lexicographical order
                filtered_children.sort_unstable_by(|first, second| first.name.cmp(&second.name));
                for subnode in filtered_children.into_iter().rev() {
                    self.stack.push((
                        current_path.clone(),
                        StreamNode {
                            node: subnode,
                            num_children: 0, // Children nodes initially have 0, their own children count is set when *they* are processed
                        },
                    ));
                }
            } else {
                // For files or symlinks, ensure num_children is 0.
                stream_node.num_children = 0;
            }

            Ok((current_path, stream_node))
        })();
        Some(res)
    }
}

/// A depth‑first *pre‑order* stream of serialized trees.
///
/// Items are produced in lexicographical order of their *full* paths. The root tree is emitted.
/// Trees are loaded from the repository as they are needed. The full tree is not stored in memory.
/// The iteration with a stack avoids recursive calls.
///
/// This stream also allows including and excluding a list of paths. Paths in the exclude list, and their
/// children, are never explored nor emitted. If the include list is not empty, only nodes in the same branch
/// (children and parents (intermediate nodes to reach the included path)) as those paths will be emitted.
pub struct SerializedTreeStream {
    repo: Arc<Repository>,
    stack: Vec<(PathBuf, ID)>,
    include: Option<Vec<PathBuf>>,
    exclude: Option<Vec<PathBuf>>,
}

impl SerializedTreeStream {
    pub fn new(
        repo: Arc<Repository>,
        root_id: &ID,
        base_path: PathBuf,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
    ) -> Result<Self> {
        Ok(Self {
            repo,
            stack: vec![(base_path, *root_id)],
            include,
            exclude,
        })
    }
}

impl Iterator for SerializedTreeStream {
    type Item = Result<(PathBuf, Tree)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (current_path, tree_id) = loop {
            match self.stack.pop() {
                None => return None,
                Some((path, id)) => {
                    if utils::filter_path(&path, self.include.as_ref(), self.exclude.as_ref()) {
                        break (path, id);
                    }
                }
            }
        };

        let res = (|| {
            let tree = Tree::load_from_repo(self.repo.as_ref(), &tree_id).with_context(|| {
                format!(
                    "Failed to load tree with ID {tree_id} for path {}",
                    current_path.display()
                )
            })?;

            // Collect children (directories only)
            let mut children = Vec::new();
            for node in tree.nodes.iter() {
                if let Some(subtree_id) = &node.tree {
                    let child_path = current_path.join(&node.name);
                    // Filter children before pushing to the stack
                    if utils::filter_path(&child_path, self.include.as_ref(), self.exclude.as_ref())
                    {
                        children.push((child_path, subtree_id));
                    }
                }
            }

            // Push children to stack in reverse lexicographical order
            children.sort_unstable_by(|(path1, _), (path2, _)| path1.cmp(path2));
            for (child_path, child_id) in children.into_iter().rev() {
                self.stack.push((child_path, *child_id));
            }

            Ok((current_path, tree))
        })();
        Some(res)
    }
}

/// Represents the type of difference found between two nodes (or lack thereof).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeDiff {
    /// The node is present in the 'next' stream but not in the 'previous' stream.
    New,
    /// The node is present in the 'previous' stream but not in the 'next' stream.
    Deleted,
    /// The node is present in both streams, but its metadata and/or contents are different.
    Changed,
    /// The node is present in both streams, and its metadata and contents are the same.
    Unchanged,
}

/// A tuple representing an item yielded by the NodeDiffStream:
/// (full path, node from 'previous' stream, node from 'next' stream, difference type).
pub type DiffTuple = (PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff);

/// A depth‑first *pre‑order* stream of node differences.
///
/// Items are produced in lexicographical order of their *full* paths. The root node is not emitted.
///
/// This treamer accepts any iterator of `(PathBuf, StreamNode)` and produces a stream of differences
/// between a `previous` stream and a `next`. The differences between two nodes can be:
///
/// - New: `next` has a node not present in `previous`.
/// - Deleted: `prev` has a node not present in `next`.
/// - Changed: `previous` and `next` share a node, but they are deemed to be different (by comparing metadata).
/// - Unchanged: `previous` and `next` share a node and they are deemed to be the same (by comparing metadata).
pub struct NodeDiffStream<P, I>
where
    P: Iterator<Item = Result<(PathBuf, StreamNode)>>,
    I: Iterator<Item = Result<(PathBuf, StreamNode)>>,
{
    prev: P,
    next: I,
    head_prev: Option<Result<(PathBuf, StreamNode)>>,
    head_next: Option<Result<(PathBuf, StreamNode)>>,
}

impl<P, I> NodeDiffStream<P, I>
where
    P: Iterator<Item = Result<(PathBuf, StreamNode)>>,
    I: Iterator<Item = Result<(PathBuf, StreamNode)>>,
{
    pub fn new(mut prev: P, mut next: I) -> Self {
        Self {
            head_prev: prev.next(),
            head_next: next.next(),
            prev,
            next,
        }
    }
}

impl<P, I> Iterator for NodeDiffStream<P, I>
where
    P: Iterator<Item = Result<(PathBuf, StreamNode)>>,
    I: Iterator<Item = Result<(PathBuf, StreamNode)>>,
{
    type Item = Result<DiffTuple>;

    fn next(&mut self) -> Option<Self::Item> {
        match (&self.head_prev, &self.head_next) {
            (None, None) => None,
            (Some(Err(_)), _) => {
                let err = self.head_prev.take().unwrap();
                self.head_prev = self.prev.next();
                Some(Err(err.unwrap_err()))
            }
            (_, Some(Err(_))) => {
                let err = self.head_next.take().unwrap();
                self.head_next = self.next.next();
                Some(Err(err.unwrap_err()))
            }
            (Some(Ok(item_a_ref)), Some(Ok(item_b_ref))) => {
                let path_a = &item_a_ref.0;
                let path_b = &item_b_ref.0;

                match path_a.cmp(path_b) {
                    Ordering::Less => {
                        let item = self.head_prev.take().unwrap().unwrap();
                        let (previous_path, previous_stream_node) = item;

                        self.head_prev = self.prev.next();

                        Some(Ok((
                            previous_path,
                            Some(previous_stream_node),
                            None,
                            NodeDiff::Deleted,
                        )))
                    }
                    Ordering::Greater => {
                        let item = self.head_next.take().unwrap().unwrap();
                        let (incoming_path, incoming_stream_node) = item;

                        self.head_next = self.next.next();

                        Some(Ok((
                            incoming_path,
                            None,
                            Some(incoming_stream_node),
                            NodeDiff::New,
                        )))
                    }
                    Ordering::Equal => {
                        let item_a = self.head_prev.take().unwrap().unwrap();
                        let (previous_path, previous_stream_node) = item_a;

                        let item_b = self.head_next.take().unwrap().unwrap();
                        let (_, incoming_stream_node) = item_b;

                        self.head_prev = self.prev.next();
                        self.head_next = self.next.next();

                        let diff_type = if previous_stream_node
                            .node
                            .metadata
                            .is_modified(&incoming_stream_node.node.metadata)
                        {
                            NodeDiff::Changed
                        } else {
                            NodeDiff::Unchanged
                        };

                        Some(Ok((
                            previous_path,
                            Some(previous_stream_node),
                            Some(incoming_stream_node),
                            diff_type,
                        )))
                    }
                }
            }
            (Some(Ok(_)), None) => {
                let item = self.head_prev.take().unwrap().unwrap();
                let (previous_path, previous_stream_node) = item;
                self.head_prev = self.prev.next();

                Some(Ok((
                    previous_path,
                    Some(previous_stream_node),
                    None,
                    NodeDiff::Deleted,
                )))
            }
            (None, Some(Ok(_))) => {
                let item = self.head_next.take().unwrap().unwrap();
                let (incoming_path, incoming_stream_node) = item;
                self.head_next = self.next.next();

                Some(Ok((
                    incoming_path,
                    None,
                    Some(incoming_stream_node),
                    NodeDiff::New,
                )))
            }
        }
    }
}

/// Returns a serialized Node in a Tree if it exists.
pub fn find_serialized_node(
    repo: &Repository,
    base_tree_id: &ID,
    path: &Path,
) -> Result<Option<Node>> {
    if path.as_os_str().is_empty() {
        return Ok(None);
    }

    let components: Vec<&str> = path
        .components()
        .map(|c| c.as_os_str().to_str().unwrap_or_default())
        .collect();

    let mut current_tree_id: ID = *base_tree_id;

    for (i, component) in components.iter().enumerate() {
        let tree = Tree::load_from_repo(repo, &current_tree_id)?;

        match tree
            .nodes
            .binary_search_by(|n| n.name.as_str().cmp(component))
        {
            Ok(idx) => {
                let node = &tree.nodes[idx];
                if i == components.len() - 1 {
                    return Ok(Some(node.clone()));
                } else {
                    current_tree_id = node.tree.ok_or_else(|| {
                        anyhow!("'{component}' no es un directorio en el árbol {current_tree_id}")
                    })?;
                }
            }
            Err(_) => return Ok(None),
        }
    }

    Ok(None)
}

/// A streaming reader over a node’s serialized data.
///
/// `SerializedNodeDataReader` exposes the contents of a `Node` as a
/// single, contiguous byte stream, implementing [`std::io::Read`] so it
/// can be consumed like a regular file.
///
/// The node’s data is stored in the repository as a sequence of blobs
/// (arbitrary-sized chunks). This reader transparently stitches those
/// blobs together in logical order, allowing callers to read across blob
/// boundaries without needing to know how the data is chunked.
///
/// The reader:
/// - keeps only **one blob** in memory at a time, making it suitable for
///   very large files (gigabytes or terabytes);
/// - uses prefix offsets to quickly determine which blob contains a given
///   position;
/// - loads each blob at most once during sequential reading;
/// - maintains an internal cursor (`pos`) representing the global offset
///   within the virtual file.
///
/// This adapter is intended for sequential reading patterns, but works
/// correctly with any consumer of the `Read` trait.
pub struct SerializedNodeDataReader {
    repo: Arc<Repository>,

    blob_ids: Vec<ID>,

    /// Total length at the start of each blob
    blob_prefix: Vec<u64>,
    total_length: u64,

    /// Global position across the whole virtual file.
    pos: u64,

    /// Cache of the currently loaded blob.
    current_blob_idx: usize,
    current_blob: Vec<u8>,
}

impl SerializedNodeDataReader {
    pub fn new(repo: Arc<Repository>, node: &Node) -> Result<Self> {
        let blobs = node
            .blobs
            .as_ref()
            .ok_or_else(|| anyhow!("Node has no blobs"))?;

        // Lookup raw blob lengths from the index.
        let index = repo.index();

        let blob_lengths: Vec<u32> = blobs
            .iter()
            .map(|id| index.get(id).expect("Blob must exist in index").raw_length)
            .collect();

        // Compute the prefix sums
        let mut prefix = Vec::with_capacity(blob_lengths.len() + 1);
        prefix.push(0);
        let mut acc = 0u64;
        for &len in &blob_lengths {
            acc += len as u64;
            prefix.push(acc);
        }

        Ok(Self {
            repo,
            blob_ids: blobs.clone(),
            blob_prefix: prefix.clone(),
            total_length: acc,

            pos: 0,

            current_blob_idx: usize::MAX, // invalid index to force load on first read
            current_blob: Vec::new(),
        })
    }

    /// Return the index of the blob containing global position `pos`.
    /// Uses binary search on `blob_prefix`.
    fn blob_at(&self, pos: u64) -> usize {
        match self.blob_prefix.binary_search(&pos) {
            Ok(i) => i,      // exact boundary → at blob i
            Err(i) => i - 1, // inside blob i-1
        }
    }

    /// Ensure the blob at `idx` is loaded into `current_blob`.
    fn ensure_blob_loaded(&mut self, idx: usize) -> Result<()> {
        if idx != self.current_blob_idx {
            let blob = self.repo.load_blob(&self.blob_ids[idx])?;
            self.current_blob = blob;
            self.current_blob_idx = idx;
        }
        Ok(())
    }
}

impl Read for SerializedNodeDataReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.pos >= self.total_length {
            return Ok(0);
        }

        let mut written = 0;

        while written < buf.len() && self.pos < self.total_length {
            // Find blob index for current position
            let idx = self.blob_at(self.pos);

            // Load the blob if needed
            self.ensure_blob_loaded(idx)
                .map_err(std::io::Error::other)?;

            // Compute offset inside this blob
            let blob_start = self.blob_prefix[idx];
            let inside = (self.pos - blob_start) as usize;

            let available = self.current_blob.len() - inside;
            let needed = buf.len() - written;
            let to_copy = available.min(needed);

            // Copy out to caller buffer
            buf[written..written + to_copy]
                .copy_from_slice(&self.current_blob[inside..inside + to_copy]);

            written += to_copy;
            self.pos += to_copy as u64;
        }

        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    // Create a filesystem tree for testing. root should be the path to a temporary folder
    fn create_tree(root: &Path) -> Result<()> {
        // dir_a
        // |____ dir0
        // |____ dir1
        // |____ dir2
        // |      |____ file1
        // |____ file0
        //
        // dir_b
        // |____ file2

        std::fs::create_dir_all(root.join("dir_a").join("dir0"))?;
        std::fs::create_dir_all(root.join("dir_a").join("dir1"))?;
        std::fs::File::create(root.join("dir_a").join("file0"))?;
        std::fs::create_dir_all(root.join("dir_a").join("dir2"))?;
        std::fs::File::create(root.join("dir_a").join("dir2").join("file1"))?;
        std::fs::create_dir(root.join("dir_b"))?;
        std::fs::File::create(root.join("dir_b").join("file2"))?;

        Ok(())
    }

    #[test]
    fn test_fs_node_stream_with_root() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let stream = FSNodeStream::from_paths(vec![tmp_path.join("dir_a")], Vec::new())?;
        let nodes: Vec<Result<(PathBuf, StreamNode)>> = stream.collect();

        assert_eq!(nodes.len(), 6);
        assert_eq!(nodes[0].as_ref().unwrap().0, tmp_path.join("dir_a"));
        assert_eq!(
            nodes[1].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir0")
        );
        assert_eq!(
            nodes[2].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir1")
        );
        assert_eq!(
            nodes[3].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2")
        );
        assert_eq!(
            nodes[4].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2").join("file1")
        );
        assert_eq!(
            nodes[5].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("file0")
        );

        Ok(())
    }

    #[test]
    fn test_fs_node_stream_with_many_roots() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let stream = FSNodeStream::from_paths(
            vec![tmp_path.join("dir_a"), tmp_path.join("dir_b")],
            Vec::new(),
        )?;
        let nodes: Vec<Result<(PathBuf, StreamNode)>> = stream.collect();

        assert_eq!(nodes.len(), 8);
        assert_eq!(nodes[0].as_ref().unwrap().0, tmp_path.join("dir_a"));
        assert_eq!(
            nodes[1].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir0")
        );
        assert_eq!(
            nodes[2].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir1")
        );
        assert_eq!(
            nodes[3].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2")
        );
        assert_eq!(
            nodes[4].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2").join("file1")
        );
        assert_eq!(
            nodes[5].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("file0")
        );
        assert_eq!(nodes[6].as_ref().unwrap().0, tmp_path.join("dir_b"));
        assert_eq!(
            nodes[7].as_ref().unwrap().0,
            tmp_path.join("dir_b").join("file2")
        );

        Ok(())
    }

    #[test]
    fn test_fs_node_stream_with_intermediate_paths() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let stream = FSNodeStream::from_paths(
            vec![
                tmp_path.join("dir_a").join("file0"),
                tmp_path.join("dir_a").join("dir2").join("file1"),
            ],
            Vec::new(),
        )?;
        let nodes: Vec<Result<(PathBuf, StreamNode)>> = stream.collect();

        assert_eq!(nodes.len(), 3);
        assert_eq!(
            nodes[0].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2")
        );
        assert_eq!(
            nodes[1].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2").join("file1")
        );
        assert_eq!(
            nodes[2].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("file0")
        );

        Ok(())
    }

    #[test]
    fn test_diff_different_trees() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let dir_a = FSNodeStream::from_paths(vec![tmp_path.join("dir_a")], Vec::new())?;
        let dir_b = FSNodeStream::from_paths(vec![tmp_path.join("dir_b")], Vec::new())?;
        let diff_stream = NodeDiffStream::new(dir_a, dir_b);
        let diffs: Vec<Result<DiffTuple>> = diff_stream.collect();

        assert_eq!(diffs.len(), 8);
        assert_eq!(diffs[0].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[1].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[2].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[3].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[4].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[5].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[6].as_ref().unwrap().3, NodeDiff::New);
        assert_eq!(diffs[7].as_ref().unwrap().3, NodeDiff::New);

        Ok(())
    }

    #[test]
    fn test_diff_same_tree() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let dir_a1 = FSNodeStream::from_paths(vec![tmp_path.join("dir_a")], Vec::new())?;
        let dir_a2 = FSNodeStream::from_paths(vec![tmp_path.join("dir_a")], Vec::new())?;
        let diff_stream = NodeDiffStream::new(dir_a1, dir_a2);
        let diffs: Vec<Result<DiffTuple>> = diff_stream.collect();

        assert_eq!(diffs.len(), 6);
        assert_eq!(diffs[0].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[1].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[2].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[3].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[4].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[5].as_ref().unwrap().3, NodeDiff::Unchanged);

        Ok(())
    }

    #[test]
    fn test_fs_node_stream_with_exclude_paths() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let stream = FSNodeStream::from_paths(
            vec![tmp_path.join("dir_a"), tmp_path.join("dir_b")],
            vec![tmp_path.join("dir_b")],
        )?;
        let nodes: Vec<Result<(PathBuf, StreamNode)>> = stream.collect();

        assert_eq!(nodes.len(), 6);
        assert_eq!(nodes[0].as_ref().unwrap().0, tmp_path.join("dir_a"));
        assert_eq!(
            nodes[1].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir0")
        );
        assert_eq!(
            nodes[2].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir1")
        );
        assert_eq!(
            nodes[3].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2")
        );
        assert_eq!(
            nodes[4].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2").join("file1")
        );
        assert_eq!(
            nodes[5].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("file0")
        );

        Ok(())
    }
}
