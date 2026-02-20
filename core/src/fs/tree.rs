use std::{
    cmp::Ordering,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
};

use anyhow::{Context, Result, anyhow};
use async_stream::try_stream;
use futures::{StreamExt, stream::Stream};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, ReadBuf};

use crate::{
    backend::WriteContents,
    fs::{calculate_lcp, filter::PathFilter, get_intermediate_paths, node::Node},
    mapache::{BlobType, ID, SaveID, defaults::DEFAULT_FSNODESTREAM_PARALLEL_STATS},
    repository::repo::Repository,
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
    pub async fn save_to_repo(&mut self, repo: &Repository) -> Result<ID> {
        let mut owned = std::mem::take(self); // Take ownership

        let (serialized_data, owned_back) = tokio::task::spawn_blocking(move || {
            owned.nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
            let bytes = serde_json::to_vec(&owned).context("Failed to serialize tree")?;
            Ok::<_, anyhow::Error>((bytes, owned))
        })
        .await
        .context("Tree serialization panicked")??;

        *self = owned_back; // Return ownership

        repo.encode_and_save_blob(
            BlobType::Tree,
            WriteContents::Owned(serialized_data),
            SaveID::CalculateID,
        )
        .await
        .context("Failed to save tree blob to repository")
    }

    /// Load a tree from the repository.
    pub async fn load_from_repo(repo: &Repository, root_id: &ID) -> Result<Tree> {
        let tree_object = repo
            .load_blob(root_id)
            .await
            .with_context(|| format!("Failed to load tree blob with ID {root_id}"))?;
        let tree: Tree = serde_json::from_slice(&tree_object)
            .with_context(|| format!("Failed to deserialize tree with ID {root_id}"))?;
        Ok(tree)
    }
}

/// Represents a file system node along with additional information needed for streaming.
#[derive(Debug)]
pub struct StreamNode {
    pub node: Node,
    /// The number of children this node has that will be yielded by the stream.
    pub num_children: usize,
}

pub type StreamNodeInfo = (PathBuf, StreamNode);

/// Internal traversal state for FSNodeStream.
#[derive(Debug)]
struct FSNodeState {
    /// Stack stores (shared_parent_path, entry_name, maybe_node) to avoid duplicating parent PathBufs.
    stack: Vec<(Arc<PathBuf>, std::ffi::OsString, Option<Node>)>,
    intermediate_paths: Vec<(PathBuf, usize, Option<Node>)>,
    filter: Arc<PathFilter>,
}

/// A depth‑first pre‑order filesystem stream.
///
/// Items are produced in lexicographical order of their full paths.
/// The root path is not emitted.
pub struct FSNodeStream {
    inner: Pin<Box<dyn Stream<Item = Result<StreamNodeInfo>> + Send>>,
}

impl FSNodeStream {
    pub async fn from_paths(paths: Vec<PathBuf>, exclude_paths: Vec<PathBuf>) -> Result<Self> {
        let mut exclude_paths = exclude_paths;
        exclude_paths.sort_unstable();
        let filter = Arc::new(PathFilter::new(None, Some(exclude_paths)));

        let mut allowed_paths = Vec::with_capacity(paths.len());
        for path in paths {
            if filter.allow(&path) {
                let node = Node::from_path_async(&path).await.with_context(|| {
                    format!("Path {} does not exist or is inaccessible", path.display())
                })?;
                allowed_paths.push((path, node));
            }
        }

        let raw_paths: Vec<PathBuf> = allowed_paths.iter().map(|(p, _)| p.clone()).collect();
        let common_root = calculate_lcp(&raw_paths, false);
        let (_, intermediate_map) = get_intermediate_paths(&common_root, &raw_paths);

        let mut intermediate_paths: Vec<(PathBuf, usize, Option<Node>)> = intermediate_map
            .into_iter()
            .filter(|(p, _)| filter.allow(p))
            .map(|(p, n)| (p, n, None))
            .collect();

        // Reverse-sorted, because we pop from the end.
        allowed_paths.sort_unstable_by(|(a, _), (b, _)| b.cmp(a));
        intermediate_paths.sort_unstable_by(|(a, _, _), (b, _, _)| b.cmp(a));

        let mut stack = Vec::with_capacity(allowed_paths.len());
        for (p, node) in allowed_paths {
            let parent = Arc::new(
                p.parent()
                    .map(|path| path.to_path_buf())
                    .unwrap_or_default(),
            );
            let name = p.file_name().unwrap_or_default().to_os_string();
            stack.push((parent, name, Some(node)));
        }

        let state = FSNodeState {
            stack,
            intermediate_paths,
            filter,
        };

        Ok(Self {
            inner: Self::make_inner_stream(state),
        })
    }

    fn make_inner_stream(
        mut state: FSNodeState,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamNodeInfo>> + Send>> {
        try_stream! {
            while state.intermediate_paths.last().is_some() || state.stack.last().is_some() {
                let take_intermediate = match (state.intermediate_paths.last(), state.stack.last()) {
                    (None, None) => unreachable!(),
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (Some((ip, _, _)), Some((parent, name, _))) => ip < &parent.join(name),
                };

                if take_intermediate {
                    let (path, num_children, maybe_node) = state.intermediate_paths.pop().unwrap();
                    if state.filter.allow(&path) {
                        let node = if let Some(n) = maybe_node {
                            n
                        } else {
                            Node::from_path_async(&path).await?
                        };
                        yield (path, StreamNode { node, num_children });
                    }
                    continue;
                }

                let (parent, name, maybe_node) = state.stack.pop().unwrap();
                let path = parent.join(&name);
                if !state.filter.allow(&path) {
                    continue;
                }

                let node = if let Some(n) = maybe_node {
                    n
                } else {
                    Node::from_path_async(&path).await?
                };
                let mut num_children = 0;

                if node.is_dir() {
                    let path_clone = path.clone();
                    let filter_clone = Arc::clone(&state.filter);

                    // Read entries to get names.
                    let children_names = tokio::task::spawn_blocking(move || {
                        let mut names = Vec::new();
                        for entry in std::fs::read_dir(&path_clone)? {
                            let entry = entry?;
                            let child_path = entry.path();
                            if filter_clone.allow(&child_path) {
                                names.push(entry.file_name());
                            }
                        }
                        names.sort_unstable();
                        Ok::<Vec<_>, anyhow::Error>(names)
                    }).await.context("Directory read panicked")??;

                    num_children = children_names.len();

                    // Yield the directory node NOW.
                    // This allows the pipeline to start processing the directory while we stat children.
                    yield (path.clone(), StreamNode { node: node.clone(), num_children });

                    if num_children > 0 {
                        // Parallelize stats for all siblings using buffered stream.
                        let shared_parent = Arc::new(path.clone());
                        let mut stat_stream = futures::stream::iter(children_names)
                            .map(|name| {
                                let parent = shared_parent.clone();
                                async move {
                                    let child_path = parent.join(&name);
                                    let node = Node::from_path_async(&child_path).await?;
                                    Ok::<_, anyhow::Error>((name, node))
                                }
                            })
                            .buffered(DEFAULT_FSNODESTREAM_PARALLEL_STATS); // Process stats in parallel.

                        let mut results = Vec::with_capacity(num_children);
                        while let Some(res) = stat_stream.next().await {
                            results.push(res?);
                        }

                        // Push to stack in reverse order.
                        for (child_name, child_node) in results.into_iter().rev() {
                            state.stack.push((shared_parent.clone(), child_name, Some(child_node)));
                        }
                    }
                    continue;
                }

                yield (path, StreamNode { node, num_children });
            }
        }.boxed()
    }
}

impl Stream for FSNodeStream {
    type Item = Result<StreamNodeInfo>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// A depth‑first pre‑order stream of serialized nodes from the repository.
pub struct SerializedNodeStream {
    inner: Pin<Box<dyn Stream<Item = Result<StreamNodeInfo>> + Send>>,
}

struct SerializedNodeState {
    repo: Arc<Repository>,
    /// Stack stores (shared_parent_path, Node) to avoid duplicating parent PathBufs.
    stack: Vec<(Arc<PathBuf>, Node)>,
    filter: Arc<PathFilter>,
}

impl SerializedNodeStream {
    pub async fn new(
        repo: Arc<Repository>,
        root_id: Option<ID>,
        base_path: PathBuf,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
    ) -> Result<Self> {
        let filter = Arc::new(PathFilter::new(include, exclude));
        let mut stack: Vec<(Arc<PathBuf>, Node)> = Vec::new();

        if let Some(id) = root_id {
            let tree = Tree::load_from_repo(&repo, &id)
                .await
                .map_err(|e| anyhow!("Failed to load root tree {id}: {e}"))?;

            let parent = Arc::new(base_path);
            for node in tree.nodes.into_iter().rev() {
                if filter.allow(&parent.join(&node.name)) {
                    stack.push((parent.clone(), node));
                }
            }
        }

        let state = SerializedNodeState {
            repo,
            stack,
            filter,
        };

        Ok(Self {
            inner: Self::make_inner_stream(state),
        })
    }

    fn make_inner_stream(
        mut state: SerializedNodeState,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamNodeInfo>> + Send>> {
        try_stream! {
            while let Some((parent, node)) = state.stack.pop() {
                let path = parent.join(&node.name);
                if !state.filter.allow(&path) {
                    continue;
                }

                let mut num_children = 0;
                if let Some(subtree_id) = &node.tree {
                    let subtree = Tree::load_from_repo(&state.repo, subtree_id).await?;

                    let shared_parent = Arc::new(path.clone());
                    for subnode in subtree.nodes.into_iter().rev() {
                        if state.filter.allow(&shared_parent.join(&subnode.name)) {
                            state.stack.push((shared_parent.clone(), subnode));
                            num_children += 1;
                        }
                    }
                }

                yield (path, StreamNode { node, num_children });
            }
        }
        .boxed()
    }
}

impl Stream for SerializedNodeStream {
    type Item = Result<StreamNodeInfo>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// A depth‑first pre‑order stream of node differences.
pub struct NodeDiffStream<P, I>
where
    P: Stream<Item = Result<StreamNodeInfo>>,
    I: Stream<Item = Result<StreamNodeInfo>>,
{
    prev: futures::stream::Peekable<Pin<Box<P>>>,
    next: futures::stream::Peekable<Pin<Box<I>>>,
}

impl<P, I> NodeDiffStream<P, I>
where
    P: Stream<Item = Result<StreamNodeInfo>>,
    I: Stream<Item = Result<StreamNodeInfo>>,
{
    pub fn new(prev: P, next: I) -> Self {
        Self {
            prev: Box::pin(prev).peekable(),
            next: Box::pin(next).peekable(),
        }
    }

    fn with_ctx(err: anyhow::Error, msg: &'static str) -> anyhow::Error {
        Err::<(), _>(err).context(msg).unwrap_err()
    }
}

impl<P, I> Stream for NodeDiffStream<P, I>
where
    P: Stream<Item = Result<StreamNodeInfo>>,
    I: Stream<Item = Result<StreamNodeInfo>>,
{
    type Item = Result<DiffTuple>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        // Peek both sides (non-consuming)
        let p_peek = futures::ready!(Pin::new(&mut this.prev).poll_peek(cx));
        let n_peek = futures::ready!(Pin::new(&mut this.next).poll_peek(cx));

        match (p_peek, n_peek) {
            (None, None) => Poll::Ready(None),

            // prev errored: consume it and return error
            (Some(Err(_)), _) => {
                let item = futures::ready!(Pin::new(&mut this.prev).poll_next(cx));
                let err = item.expect("peeked Some").unwrap_err();
                Poll::Ready(Some(Err(Self::with_ctx(err, "Error in 'previous' stream"))))
            }

            // next errored: consume it and return error
            (_, Some(Err(_))) => {
                let item = futures::ready!(Pin::new(&mut this.next).poll_next(cx));
                let err = item.expect("peeked Some").unwrap_err();
                Poll::Ready(Some(Err(Self::with_ctx(err, "Error in 'next' stream"))))
            }

            // both Ok: compare paths
            (Some(Ok((path_p, _))), Some(Ok((path_n, _)))) => match path_p.cmp(path_n) {
                Ordering::Less => {
                    let item = futures::ready!(Pin::new(&mut this.prev).poll_next(cx));
                    let (path, node) = item.expect("peeked Some").expect("peeked Ok");
                    Poll::Ready(Some(Ok((path, Some(node), None, NodeDiff::Deleted))))
                }
                Ordering::Greater => {
                    let item = futures::ready!(Pin::new(&mut this.next).poll_next(cx));
                    let (path, node) = item.expect("peeked Some").expect("peeked Ok");
                    Poll::Ready(Some(Ok((path, None, Some(node), NodeDiff::New))))
                }
                Ordering::Equal => {
                    let p_item = futures::ready!(Pin::new(&mut this.prev).poll_next(cx));
                    let n_item = futures::ready!(Pin::new(&mut this.next).poll_next(cx));

                    let (path, node_p) = p_item.expect("peeked Some").expect("peeked Ok");
                    let (_, node_n) = n_item.expect("peeked Some").expect("peeked Ok");

                    let diff = if node_p.node.metadata.is_modified(&node_n.node.metadata) {
                        NodeDiff::Changed
                    } else {
                        NodeDiff::Unchanged
                    };

                    Poll::Ready(Some(Ok((path, Some(node_p), Some(node_n), diff))))
                }
            },

            // only prev left
            (Some(Ok(_)), None) => {
                let item = futures::ready!(Pin::new(&mut this.prev).poll_next(cx));
                let (path, node) = item.expect("peeked Some").expect("peeked Ok");
                Poll::Ready(Some(Ok((path, Some(node), None, NodeDiff::Deleted))))
            }

            // only next left
            (None, Some(Ok(_))) => {
                let item = futures::ready!(Pin::new(&mut this.next).poll_next(cx));
                let (path, node) = item.expect("peeked Some").expect("peeked Ok");
                Poll::Ready(Some(Ok((path, None, Some(node), NodeDiff::New))))
            }
        }
    }
}

pub type DiffTuple = (PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeDiff {
    New,
    Deleted,
    Changed,
    Unchanged,
}

/// The internal state of the serialized tree stream.
struct SerializedTreeState {
    repo: Arc<Repository>,
    stack: Vec<(Arc<PathBuf>, ID)>,
    filter: Arc<PathFilter>,
}

/// A depth‑first pre‑order stream of serialized trees.
#[allow(clippy::type_complexity)]
pub struct SerializedTreeStream {
    inner: Pin<Box<dyn Stream<Item = Result<(PathBuf, Tree)>> + Send>>,
}

impl SerializedTreeStream {
    pub async fn new(
        repo: Arc<Repository>,
        root_id: &ID,
        base_path: PathBuf,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
    ) -> Result<Self> {
        let filter = Arc::new(PathFilter::new(include, exclude));
        let stack = vec![(Arc::new(base_path), *root_id)];

        let state = SerializedTreeState {
            repo,
            stack,
            filter,
        };

        Ok(Self {
            inner: Self::make_inner_stream(state),
        })
    }

    #[allow(clippy::type_complexity)]
    fn make_inner_stream(
        mut state: SerializedTreeState,
    ) -> Pin<Box<dyn Stream<Item = Result<(PathBuf, Tree)>> + Send>> {
        try_stream! {
            while let Some((current_path, tree_id)) = state.stack.pop() {
                // Skip if filter doesn't allow this branch
                if !state.filter.allow(&current_path) {
                    continue;
                }

                // Load the tree from the repository
                let tree = Tree::load_from_repo(&state.repo, &tree_id).await?;

                // Push children (dirs only) to the stack in reverse order
                // This ensures lexicographical pre-order traversal
                let shared_parent = Arc::new((*current_path).clone());
                for node in tree.nodes.iter().rev() {
                    if let Some(subtree_id) = &node.tree {
                        let child_path = shared_parent.join(&node.name);

                        if state.filter.allow(&child_path) {
                            state.stack.push((Arc::new(child_path), *subtree_id));
                        }
                    }
                }

                yield ((*current_path).clone(), tree);
            }
        }
        .boxed()
    }
}

impl Stream for SerializedTreeStream {
    type Item = Result<(PathBuf, Tree)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

/// A streaming reader over a node’s serialized data from the repository.
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
    #[allow(clippy::type_complexity)]
    pending_load: Option<futures::future::BoxFuture<'static, Result<(usize, Vec<u8>)>>>,
}

impl SerializedNodeDataReader {
    pub async fn new(repo: Arc<Repository>, node: &Node) -> Result<Self> {
        let blobs = node
            .blobs
            .as_ref()
            .ok_or_else(|| anyhow!("Node has no blobs"))?;
        let index = repo.index();
        let mut prefix = vec![0];
        let mut acc = 0u64;

        for id in blobs {
            let entry = index
                .get(id)
                .ok_or_else(|| anyhow!("Blob {id} not found"))?;
            acc += entry.raw_length as u64;
            prefix.push(acc);
        }

        Ok(Self {
            repo,
            blob_ids: blobs.clone(),
            blob_prefix: prefix,
            total_length: acc,
            pos: 0,
            current_blob_idx: usize::MAX,
            current_blob: Vec::new(),
            pending_load: None,
        })
    }

    fn blob_at(&self, pos: u64) -> usize {
        match self.blob_prefix.binary_search(&pos) {
            Ok(i) => i,
            Err(i) => i - 1,
        }
    }
}

impl AsyncRead for SerializedNodeDataReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.pos >= self.total_length || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let target_idx = self.blob_at(self.pos);

        if self.current_blob_idx != target_idx {
            if self.pending_load.is_none() {
                let repo = self.repo.clone();
                let blob_id = self.blob_ids[target_idx];
                self.pending_load = Some(Box::pin(async move {
                    let data = repo.load_blob(&blob_id).await?;
                    Ok((target_idx, data))
                }));
            }

            if let Some(ref mut fut) = self.pending_load {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok((idx, data))) => {
                        self.current_blob = data;
                        self.current_blob_idx = idx;
                        self.pending_load = None;
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(std::io::Error::other(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        let blob_start = self.blob_prefix[self.current_blob_idx];
        let offset = (self.pos - blob_start) as usize;
        let to_copy = (self.current_blob.len() - offset).min(buf.remaining());

        buf.put_slice(&self.current_blob[offset..offset + to_copy]);
        self.pos += to_copy as u64;

        Poll::Ready(Ok(()))
    }
}

pub async fn find_serialized_node(
    repo: &Repository,
    base_tree_id: &ID,
    path: &Path,
) -> Result<Option<Node>> {
    if path.as_os_str().is_empty() {
        return Ok(None);
    }

    let mut current_tree_id: ID = *base_tree_id;
    let mut components = path.components().peekable();

    while let Some(component) = components.next() {
        let name = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| anyhow!("Path component contains invalid UTF-8: {:?}", component))?;

        let tree = Tree::load_from_repo(repo, &current_tree_id).await?;

        match tree.nodes.binary_search_by(|n| n.name.as_str().cmp(name)) {
            Ok(idx) => {
                let node = &tree.nodes[idx];
                if components.peek().is_none() {
                    return Ok(Some(node.clone()));
                } else {
                    current_tree_id = node.tree.ok_or_else(|| {
                        anyhow!(
                            "Path component '{name}' is not a directory in tree {current_tree_id}"
                        )
                    })?;
                }
            }
            Err(_) => return Ok(None),
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use tempfile::tempdir;

    use anyhow::Result;
    use futures::StreamExt; // for collect(), boxed_local()

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

    #[tokio::test]
    async fn test_fs_node_stream_with_root() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let nodes: Vec<Result<(PathBuf, StreamNode)>> =
            FSNodeStream::from_paths(vec![tmp_path.join("dir_a")], Vec::new())
                .await?
                .collect()
                .await;

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

    #[tokio::test]
    async fn test_fs_node_stream_with_many_roots() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let nodes: Vec<Result<(PathBuf, StreamNode)>> = FSNodeStream::from_paths(
            vec![tmp_path.join("dir_a"), tmp_path.join("dir_b")],
            Vec::new(),
        )
        .await?
        .collect()
        .await;

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

    #[tokio::test]
    async fn test_fs_node_stream_with_intermediate_paths() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let nodes: Vec<Result<(PathBuf, StreamNode)>> = FSNodeStream::from_paths(
            vec![
                tmp_path.join("dir_a").join("file0"),
                tmp_path.join("dir_a").join("dir2").join("file1"),
            ],
            Vec::new(),
        )
        .await?
        .collect()
        .await;

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

    #[tokio::test]
    async fn test_diff_different_trees() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        // Box the streams so they satisfy NodeDiffStream's `Unpin` bounds.
        let dir_a = FSNodeStream::from_paths(vec![tmp_path.join("dir_a")], Vec::new())
            .await?
            .boxed_local();

        let dir_b = FSNodeStream::from_paths(vec![tmp_path.join("dir_b")], Vec::new())
            .await?
            .boxed_local();

        let diffs: Vec<Result<DiffTuple>> = NodeDiffStream::new(dir_a, dir_b).collect().await;

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

    #[tokio::test]
    async fn test_diff_same_tree() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let dir_a1 = FSNodeStream::from_paths(vec![tmp_path.join("dir_a")], Vec::new())
            .await?
            .boxed_local();

        let dir_a2 = FSNodeStream::from_paths(vec![tmp_path.join("dir_a")], Vec::new())
            .await?
            .boxed_local();

        let diffs: Vec<Result<DiffTuple>> = NodeDiffStream::new(dir_a1, dir_a2).collect().await;

        assert_eq!(diffs.len(), 6);
        assert_eq!(diffs[0].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[1].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[2].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[3].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[4].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[5].as_ref().unwrap().3, NodeDiff::Unchanged);

        Ok(())
    }

    #[tokio::test]
    async fn test_fs_node_stream_with_exclude_paths() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let nodes: Vec<Result<(PathBuf, StreamNode)>> = FSNodeStream::from_paths(
            vec![tmp_path.join("dir_a"), tmp_path.join("dir_b")],
            vec![tmp_path.join("dir_b")],
        )
        .await?
        .collect()
        .await;

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
