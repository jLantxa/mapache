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
    repository::{
        repo::{Repository, SizePair},
        storage::EncodingContext,
    },
    utils::{self, filter::PathFilter},
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
    pub fn save_to_repo(
        &mut self,
        repo: &Repository,
        encoding_context: &mut EncodingContext,
    ) -> Result<(ID, SizePair)> {
        // Sort all nodes by name before serializing
        self.nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        // Reserve some space for each node's text. This value is a heuristic.
        let mut buffer = Vec::with_capacity(self.nodes.len() * 160);
        serde_json::to_writer(&mut buffer, self).context("Failed to serialize tree nodes")?;

        let (id, data_size, meta_size) = repo.encode_and_save_blob(
            encoding_context,
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
    stack: Vec<(PathBuf, StreamNode)>,
    intermediate_paths: Vec<(PathBuf, usize)>,
    filter: PathFilter,

    // reused buffer: (file_name, direntry)
    scratch: Vec<(std::ffi::OsString, std::fs::DirEntry)>,
}

impl FSNodeStream {
    pub fn from_paths(mut paths: Vec<PathBuf>, mut exclude_paths: Vec<PathBuf>) -> Result<Self> {
        for path in &paths {
            if !fs::path_exists(path) {
                bail!("Path {} does not exist", path.display());
            }
        }

        exclude_paths.sort_unstable();
        let filter = PathFilter::new(None, Some(exclude_paths.as_slice()));

        // Keep only allowed roots
        paths.retain(|p| filter.allow(p));

        let common_root = utils::calculate_lcp(&paths, false);
        let (_root_children_count, intermediate_map) =
            utils::get_intermediate_paths(&common_root, &paths);

        // Prefilter intermediate paths once (no need to re-check in next()).
        let mut intermediate_paths: Vec<(PathBuf, usize)> = intermediate_map
            .into_iter()
            .filter(|(p, _)| filter.allow(p))
            .collect();

        // reverse for pop()
        paths.sort_unstable_by(|a, b| b.cmp(a));
        intermediate_paths.sort_unstable_by(|(a, _), (b, _)| b.cmp(a));

        // Stack holds full paths. Root nodes are "uninitialized" (name == ""),
        // so we'll stat them once when popped.
        let mut stack = Vec::with_capacity(paths.len());
        for p in paths {
            stack.push((
                p,
                StreamNode {
                    node: Node::default(), // sentinel: name == ""
                    num_children: 0,
                },
            ));
        }

        Ok(Self {
            stack,
            intermediate_paths,
            filter,
            scratch: Vec::with_capacity(256),
        })
    }

    #[inline]
    fn fill_children_sorted(&mut self, dir: &Path) -> Result<()> {
        self.scratch.clear();

        let rd = std::fs::read_dir(dir).with_context(|| format!("Cannot read {:?}.", dir))?;
        for e in rd {
            let e = e?;
            self.scratch.push((e.file_name(), e));
        }

        // Keep your global lexicographic-by-full-path order determinism.
        self.scratch.sort_unstable_by(|(na, _), (nb, _)| na.cmp(nb));
        Ok(())
    }
}

impl Iterator for FSNodeStream {
    type Item = Result<StreamNodeInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        // We only need a loop to skip filtered items.
        while let (Some(_), _) | (_, Some(_)) = (self.intermediate_paths.last(), self.stack.last())
        {
            // Choose next item in full-path lexical order:
            let take_intermediate = match (self.intermediate_paths.last(), self.stack.last()) {
                (None, None) => unreachable!(),
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((ip, _)), Some((sp, _))) => ip < sp,
            };

            if take_intermediate {
                let (path, num_children) = self.intermediate_paths.pop().unwrap();
                if !self.filter.allow(&path) {
                    continue;
                }
                return Some(
                    Node::from_path(&path).map(|node| (path, StreamNode { node, num_children })),
                );
            }

            // Pop next path
            let (path, mut stream_node) = self.stack.pop().unwrap();
            if !self.filter.allow(&path) {
                continue;
            }

            return Some((|| {
                let node = Node::from_path(&path)?;
                stream_node.node = node;

                if stream_node.node.is_dir() {
                    self.fill_children_sorted(&path)?;
                    let mut count = 0usize;

                    for (_name, e) in self.scratch.iter().rev() {
                        let child_path = e.path();
                        if !self.filter.allow(&child_path) {
                            continue;
                        }

                        let child_node = Node::from_dir_entry(&child_path, e)?;
                        self.stack.push((
                            child_path,
                            StreamNode {
                                node: child_node,
                                num_children: 0,
                            },
                        ));
                        count += 1;
                    }

                    stream_node.num_children = count;
                } else {
                    stream_node.num_children = 0;
                }

                Ok((path, stream_node))
            })());
        }

        None
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
    stack: Vec<StreamNodeInfo>, // (full path, StreamNode)
    filter: PathFilter,
}

impl SerializedNodeStream {
    pub fn new(
        repo: Arc<Repository>,
        root_id: Option<ID>,
        base_path: PathBuf,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
    ) -> Result<Self> {
        let filter = PathFilter::new(include.as_deref(), exclude.as_deref());
        let mut stack: Vec<StreamNodeInfo> = Vec::new();

        if let Some(id) = root_id {
            let tree = Tree::load_from_repo(repo.as_ref(), &id)
                .with_context(|| format!("Failed to load root tree with ID {id}"))?;

            // Tree nodes are expected to be sorted by name on disk (save_to_repo does it).
            // We push in reverse so pop() yields lexicographically smallest first.
            for node in tree.nodes.into_iter().rev() {
                let full_path = base_path.join(&node.name);
                if filter.allow(&full_path) {
                    stack.push((
                        full_path,
                        StreamNode {
                            node,
                            num_children: 0,
                        },
                    ));
                }
            }
        }

        Ok(Self {
            repo,
            stack,
            filter,
        })
    }
}

impl Iterator for SerializedNodeStream {
    type Item = Result<StreamNodeInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        // pop next allowed node (stack was prefiltered, but children are filtered too)
        let (current_path, mut stream_node) = loop {
            match self.stack.pop() {
                None => return None,
                Some((path, node)) => {
                    // Keep correctness even if some caller pushes unfiltered items later
                    if self.filter.allow(&path) {
                        break (path, node);
                    }
                }
            }
        };

        let res = (|| {
            if let Some(subtree_id) = &stream_node.node.tree {
                let subtree = Tree::load_from_repo(self.repo.as_ref(), subtree_id)?;

                let mut pushed = 0usize;

                // Push children in reverse lexicographic name order so that pop() yields
                // lexicographically smallest full paths first.
                for subnode in subtree.nodes.into_iter().rev() {
                    let child_path = current_path.join(&subnode.name);
                    if self.filter.allow(&child_path) {
                        self.stack.push((
                            child_path,
                            StreamNode {
                                node: subnode,
                                num_children: 0,
                            },
                        ));
                        pushed += 1;
                    }
                }

                stream_node.num_children = pushed;
            } else {
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
    filter: PathFilter,
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
            filter: PathFilter::new(include.as_deref(), exclude.as_deref()),
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
                    if self.filter.allow(&path) {
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

            // Push children (dirs only), reverse order.
            for node in tree.nodes.iter().rev() {
                if let Some(subtree_id) = &node.tree {
                    let child_path = current_path.join(&node.name);
                    if self.filter.allow(&child_path) {
                        self.stack.push((child_path, *subtree_id));
                    }
                }
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
                        anyhow!("'{component}' is not a directory in tree {current_tree_id}")
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
