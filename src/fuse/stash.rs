use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Result, anyhow, bail};
use fuser::{FUSE_ROOT_ID, FileAttr};

use crate::{
    fs::node::{Node, NodeType},
    fuse::{
        cache::{BlobCache, TreeCache},
        fs::Inode,
    },
    mapache::ID,
    repository::repo::Repository,
    utils::size,
};

pub(super) const BLKSIZE: u32 = 512;
pub(super) const TTL: Duration = Duration::from_secs(60);
pub(super) const TREE_CACHE_CAPACITY: usize = 512;
pub(super) const BLOB_CACHE_CAPACITY: u64 = 64 * size::MiB;

#[derive(Debug)]
pub(super) enum FsNode {
    Root {
        attr: FileAttr,
        children: BTreeMap<String, Inode>,
    },
    Dir {
        name: String,
        parent_ino: Inode,
        attr: FileAttr,
        children: BTreeMap<String, Inode>,
    },
    Symlink {
        name: String,
        parent_ino: Inode,
        attr: FileAttr,
        target: String,
    },
    SnapshotRoot {
        name: String,
        tree_id: ID,
        attr: FileAttr,
        parent_ino: Inode,
    },
    TreeNode {
        tree_id: Option<ID>,
        blobs: Option<Vec<ID>>,
        symlink_target: Option<String>,
        parent_ino: Inode,
        attr: FileAttr,
    },
}

pub(super) struct Stash {
    repo: Arc<Repository>,

    ino_counter: Inode,
    nodes: BTreeMap<Inode, FsNode>,
    path_cache: BTreeMap<(Inode, String), Inode>,
    tree_cache: TreeCache,
    blob_cache: BlobCache,
}

impl Stash {
    pub(super) fn new_root(repo: Arc<Repository>) -> Result<Self> {
        let root_attr = build_dir_attr(FUSE_ROOT_ID, repo.manifest().created_time().into());

        let mut stash = Self {
            repo: repo.clone(),
            ino_counter: FUSE_ROOT_ID,
            nodes: BTreeMap::new(),
            path_cache: BTreeMap::new(),
            tree_cache: TreeCache::new(repo.clone(), TREE_CACHE_CAPACITY),
            blob_cache: BlobCache::new(repo.clone(), BLOB_CACHE_CAPACITY),
        };

        stash.nodes.insert(
            FUSE_ROOT_ID,
            FsNode::Root {
                attr: root_attr,
                children: BTreeMap::new(),
            },
        );

        Ok(stash)
    }

    pub(super) fn add_dir(&mut self, parent_ino: Inode, dir_name: String) -> Inode {
        let ino = self.next_ino();
        let created_time: SystemTime = self.repo.manifest().created_time().into();

        let attr = build_dir_attr(ino, created_time);

        let node = FsNode::Dir {
            name: dir_name.clone(),
            parent_ino,
            attr,
            children: BTreeMap::new(),
        };

        self.nodes.insert(ino, node);
        self.path_cache.insert((parent_ino, dir_name.clone()), ino);
        self.insert_child(parent_ino, dir_name, ino);

        ino
    }

    pub(super) fn add_snapshot_dir(
        &mut self,
        parent_ino: Inode,
        dir_name: String,
        tree_id: ID,
    ) -> Inode {
        let ino = self.next_ino();
        let created_time: SystemTime = self.repo.manifest().created_time().into();

        let attr = build_dir_attr(ino, created_time);

        let node = FsNode::SnapshotRoot {
            name: dir_name.clone(),
            tree_id,
            attr,
            parent_ino,
        };

        self.nodes.insert(ino, node);
        self.path_cache.insert((parent_ino, dir_name.clone()), ino);
        self.insert_child(parent_ino, dir_name.clone(), ino);

        ino
    }

    pub(super) fn add_symlink(&mut self, parent_ino: Inode, name: String, target: String) -> Inode {
        let ino = self.next_ino();
        let created_time: SystemTime = self.repo.manifest().created_time().into();

        let attr = build_symlink_attr(ino, created_time, &target);

        let node = FsNode::Symlink {
            name: name.clone(),
            parent_ino,
            attr,
            target,
        };

        self.nodes.insert(ino, node);
        self.path_cache.insert((parent_ino, name.clone()), ino);
        self.insert_child(parent_ino, name, ino);

        ino
    }

    fn next_ino(&mut self) -> Inode {
        self.ino_counter += 1;
        self.ino_counter
    }

    fn insert_child(&mut self, parent_ino: Inode, name: String, child_ino: Inode) {
        if let Some(FsNode::Root { attr, children }) | Some(FsNode::Dir { attr, children, .. }) =
            self.nodes.get_mut(&parent_ino)
        {
            attr.nlink += 1;
            children.insert(name, child_ino);
        }
    }

    pub(super) fn lookup(&mut self, parent_ino: Inode, name: String) -> Option<&FileAttr> {
        // Check path cache first
        if let Some(ino) = self.path_cache.get(&(parent_ino, name.clone())) {
            return match self.nodes.get(ino)? {
                FsNode::Root { attr, .. }
                | FsNode::Dir { attr, .. }
                | FsNode::SnapshotRoot { attr, .. }
                | FsNode::TreeNode { attr, .. }
                | FsNode::Symlink { attr, .. } => Some(attr),
            };
        }

        let parent_node = self.nodes.get(&parent_ino)?;
        let tree_id_to_load = match parent_node {
            FsNode::SnapshotRoot { tree_id, .. } => Some(tree_id),
            FsNode::TreeNode {
                tree_id: Some(id),
                attr,
                ..
            } if attr.kind == fuser::FileType::Directory => Some(id),
            _ => None,
        };

        if let Some(tree_id) = tree_id_to_load {
            let tree = self.tree_cache.load(tree_id).ok()?.clone();
            let parent_create_time = match parent_node {
                FsNode::SnapshotRoot { attr, .. } => attr.crtime,
                FsNode::TreeNode { attr, .. } => attr.crtime,
                _ => self.repo.manifest().created_time().into(),
            };

            for node in tree.nodes.iter() {
                let ino = self.next_ino();
                let file_attr = node_to_fileattr(ino, parent_create_time, node);

                let fs_node = FsNode::TreeNode {
                    tree_id: node.tree,
                    blobs: node.blobs.clone(),
                    symlink_target: node
                        .symlink_info
                        .clone()
                        .map(|info| info.target_path.to_string_lossy().to_string()),
                    parent_ino,
                    attr: file_attr,
                };

                self.nodes.insert(ino, fs_node);
                self.path_cache.insert((parent_ino, node.name.clone()), ino);

                self.nodes
                    .entry(parent_ino)
                    .and_modify(|p_node| match p_node {
                        FsNode::Root { children, .. } | FsNode::Dir { children, .. } => {
                            children.insert(node.name.clone(), ino);
                        }
                        _ => {}
                    });

                if node.name == name {
                    return match self.nodes.get(&ino) {
                        Some(FsNode::TreeNode { attr, .. }) => Some(attr),
                        _ => None,
                    };
                }
            }
        }

        None
    }

    pub(super) fn get_attr(&self, ino: Inode) -> Option<FileAttr> {
        let node = self.nodes.get(&ino)?;

        match node {
            FsNode::Root { attr, .. }
            | FsNode::Dir { attr, .. }
            | FsNode::SnapshotRoot { attr, .. }
            | FsNode::TreeNode { attr, .. }
            | FsNode::Symlink { attr, .. } => Some(*attr),
        }
    }

    pub(super) fn read_dir(
        &mut self,
        ino: Inode,
        offset: i64,
    ) -> Vec<(Inode, fuser::FileType, String)> {
        let node = match self.nodes.get(&ino) {
            Some(node) => node,
            None => return Vec::new(),
        };

        let mut entries: Vec<(Inode, fuser::FileType, String)> = Vec::new();
        let mut current_internal_offset = 0;

        let (parent_ino_for_dotdot, tree_id_to_load) = match node {
            FsNode::Root { children, .. } => {
                if offset <= current_internal_offset {
                    let self_attr = self
                        .get_attr(ino)
                        .expect("Directory's own attribute not found!");
                    entries.push((self_attr.ino, self_attr.kind, ".".to_string()));
                }
                current_internal_offset += 1;

                if offset <= current_internal_offset {
                    let parent_attr = self
                        .get_attr(FUSE_ROOT_ID)
                        .expect("Parent's attribute not found!");
                    entries.push((parent_attr.ino, parent_attr.kind, "..".to_string()));
                }
                current_internal_offset += 1;

                for &child_ino in children.values() {
                    if offset > current_internal_offset {
                        current_internal_offset += 1;
                        continue;
                    }
                    if let Some(child_node) = self.nodes.get(&child_ino) {
                        let (kind, actual_name) = match child_node {
                            FsNode::Dir { attr, name, .. } => (attr.kind, name),
                            FsNode::SnapshotRoot { name, .. } => (fuser::FileType::Directory, name),
                            _ => continue, // Should only contain Dir or SnapshotRoot
                        };
                        entries.push((child_ino, kind, actual_name.clone()));
                    }
                    current_internal_offset += 1;
                }
                return entries;
            }
            FsNode::Dir {
                parent_ino,
                children,
                ..
            } => {
                if offset <= current_internal_offset {
                    let self_attr = self
                        .get_attr(ino)
                        .expect("Directory's own attribute not found!");
                    entries.push((self_attr.ino, self_attr.kind, ".".to_string()));
                }
                current_internal_offset += 1;

                if offset <= current_internal_offset {
                    let parent_attr = self
                        .get_attr(*parent_ino)
                        .expect("Parent's attribute not found!");
                    entries.push((parent_attr.ino, parent_attr.kind, "..".to_string()));
                }
                current_internal_offset += 1;

                for &child_ino in children.values() {
                    if offset > current_internal_offset {
                        current_internal_offset += 1;
                        continue;
                    }
                    if let Some(child_node) = self.nodes.get(&child_ino) {
                        let (kind, actual_name) = match child_node {
                            FsNode::Dir { attr, name, .. } => (attr.kind, name),
                            FsNode::SnapshotRoot { name, .. } => (fuser::FileType::Directory, name),
                            FsNode::Symlink { name, .. } => (fuser::FileType::Symlink, name),
                            _ => continue, // Should only contain Dir or SnapshotRoot
                        };
                        entries.push((child_ino, kind, actual_name.clone()));
                    }
                    current_internal_offset += 1;
                }
                return entries;
            }
            FsNode::SnapshotRoot {
                tree_id,
                parent_ino,
                ..
            } => (*parent_ino, Some(tree_id)),
            FsNode::TreeNode {
                tree_id: Some(id),
                parent_ino,
                attr,
                ..
            } if attr.kind == fuser::FileType::Directory => (*parent_ino, Some(id)),
            FsNode::Symlink { parent_ino, .. } => (*parent_ino, None),
            _ => return Vec::new(),
        };

        if offset <= current_internal_offset {
            let self_attr = self
                .get_attr(ino)
                .expect("Directory's own attribute not found!");
            entries.push((self_attr.ino, self_attr.kind, ".".to_string()));
        }
        current_internal_offset += 1;

        if offset <= current_internal_offset {
            let parent_attr = self
                .get_attr(parent_ino_for_dotdot)
                .expect("Parent's attribute not found!");
            entries.push((parent_attr.ino, parent_attr.kind, "..".to_string()));
        }
        current_internal_offset += 1;

        if let Some(tree_id) = tree_id_to_load.cloned() {
            let tree = self
                .tree_cache
                .load(&tree_id)
                .expect("Failed to load tree")
                .clone();
            let parent_create_time = self
                .get_attr(ino)
                .map(|attr| attr.crtime)
                .unwrap_or(self.repo.manifest().created_time().into());

            for node in tree.nodes.iter() {
                let child_ino_result = self.path_cache.get(&(ino, node.name.clone()));
                let child_ino = match child_ino_result {
                    Some(&cached_ino) => cached_ino,
                    None => {
                        let new_ino = self.next_ino();
                        let file_attr = node_to_fileattr(new_ino, parent_create_time, node);
                        let fs_node = FsNode::TreeNode {
                            tree_id: node.tree,
                            blobs: node.blobs.clone(),
                            symlink_target: node
                                .symlink_info
                                .clone()
                                .map(|info| info.target_path.to_string_lossy().to_string()),
                            parent_ino: ino,
                            attr: file_attr,
                        };
                        self.nodes.insert(new_ino, fs_node);
                        self.path_cache.insert((ino, node.name.clone()), new_ino);
                        new_ino
                    }
                };

                if offset > current_internal_offset {
                    current_internal_offset += 1;
                    continue;
                }

                let file_type = match node.node_type {
                    NodeType::File => fuser::FileType::RegularFile,
                    NodeType::Directory => fuser::FileType::Directory,
                    NodeType::Symlink => fuser::FileType::Symlink,
                    NodeType::BlockDevice => fuser::FileType::BlockDevice,
                    NodeType::CharDevice => fuser::FileType::CharDevice,
                    NodeType::Fifo => fuser::FileType::NamedPipe,
                    NodeType::Socket => fuser::FileType::Socket,
                };

                entries.push((child_ino, file_type, node.name.clone()));
                current_internal_offset += 1;
            }
        }

        entries
    }

    // New method to read the target of a symlink
    pub(super) fn read_link(&self, ino: Inode) -> Result<String> {
        match self.nodes.get(&ino) {
            Some(FsNode::Symlink { target, .. }) => Ok(target.clone()),
            Some(FsNode::TreeNode { symlink_target, .. }) => match symlink_target {
                Some(target) => Ok(target.to_string()),
                None => Err(anyhow!("Inode is not a symlink")),
            },
            _ => Err(anyhow!("Inode is not a symlink or does not exist")),
        }
    }

    /// Reads content of a file node.
    pub(super) fn read_from_file(
        &mut self,
        ino: Inode,
        offset: i64,
        mut size: u32,
    ) -> Result<Option<Vec<u8>>> {
        let blob_ids: &Vec<ID> = match self.nodes.get(&ino) {
            Some(FsNode::TreeNode {
                blobs: Some(ids), ..
            }) => ids,
            Some(FsNode::TreeNode { blobs: None, .. }) => {
                bail!("Node with ino {ino} has no contents")
            }
            Some(_) => bail!("Node with ino {ino} is not a snapshot node"),
            None => return Ok(None),
        };

        let index_guard = self.repo.index();
        let index = index_guard.read();

        let mut current_offset = 0;
        let mut data = Vec::with_capacity(size as usize);

        for blob_id in blob_ids {
            if size == 0 {
                break;
            }

            let indexed_blob_descriptor = index.get(blob_id);
            let locator = match indexed_blob_descriptor {
                Some(desc) => desc,
                None => bail!("Node with ino {ino} has unreferenced blobs (blob_id: {blob_id})"),
            };

            if current_offset + (locator.raw_length as i64) < offset {
                // We didn't reach the offset yet or are exactly at its end
                current_offset += locator.raw_length as i64;
                continue;
            }

            let start_in_blob = if current_offset < offset {
                offset - current_offset
            } else {
                0
            };

            let bytes_available_in_blob = locator.raw_length as i64 - start_in_blob;
            let bytes_to_read_from_blob = std::cmp::min(size as i64, bytes_available_in_blob);

            if bytes_to_read_from_blob <= 0 {
                // Should never happen, but...
                current_offset += locator.raw_length as i64;
                continue;
            }

            let blob = match self.blob_cache.load(blob_id) {
                Ok(data) => data,
                Err(e) => bail!("Failed to cache blob {blob_id}: {e}"),
            };

            // Ensure the slice is within bounds (defensive check)
            let end_in_blob = start_in_blob + bytes_to_read_from_blob;
            if end_in_blob > blob.len() as i64 {
                bail!("Blob data inconsistency: requested slice out of bounds for blob {blob_id}");
            }

            data.extend_from_slice(&blob[start_in_blob as usize..end_in_blob as usize]);

            current_offset += end_in_blob;
            size -= bytes_to_read_from_blob as u32;
        }

        Ok(Some(data))
    }
}

fn node_to_fileattr(ino: Inode, parent_create_time: SystemTime, node: &Node) -> FileAttr {
    let kind = match node.node_type {
        NodeType::File => fuser::FileType::RegularFile,
        NodeType::Directory => fuser::FileType::Directory,
        NodeType::Symlink => fuser::FileType::Symlink,
        NodeType::BlockDevice => fuser::FileType::BlockDevice,
        NodeType::CharDevice => fuser::FileType::CharDevice,
        NodeType::Fifo => fuser::FileType::NamedPipe,
        NodeType::Socket => fuser::FileType::Socket,
    };

    let size = if kind == fuser::FileType::RegularFile {
        node.metadata.size
    } else {
        0
    };
    let blocks = if kind == fuser::FileType::RegularFile {
        size.div_ceil(BLKSIZE as u64)
    } else {
        0
    };
    let perm = node.metadata.mode.unwrap_or(0o755) as u16;
    let nlink = if kind == fuser::FileType::Directory {
        2
    } else {
        1
    }; // Directories usually have 2 links (. and ..)

    FileAttr {
        ino,
        size,
        blocks,
        atime: node.metadata.accessed_time.unwrap_or(parent_create_time),
        mtime: node.metadata.modified_time.unwrap_or(parent_create_time),
        ctime: node.metadata.created_time.unwrap_or(parent_create_time),
        crtime: node.metadata.created_time.unwrap_or(parent_create_time),
        kind,
        perm,
        nlink,
        uid: node.metadata.owner_uid.unwrap_or(0),
        gid: node.metadata.owner_gid.unwrap_or(0),
        rdev: node.metadata.rdev.unwrap_or(0) as u32,
        blksize: BLKSIZE,
        flags: 0,
    }
}

/// Utility function to create a directory FileAttr
fn build_dir_attr(ino: Inode, created_time: SystemTime) -> FileAttr {
    FileAttr {
        ino,
        size: 0,
        blocks: 0,
        atime: created_time,
        mtime: created_time,
        ctime: created_time,
        crtime: created_time,
        kind: fuser::FileType::Directory,
        perm: 0o755,
        nlink: 2,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: BLKSIZE,
        flags: 0,
    }
}

/// Utility function to create a symlink FileAttr
fn build_symlink_attr(ino: Inode, created_time: SystemTime, target: &str) -> FileAttr {
    FileAttr {
        ino,
        size: target.len() as u64,
        blocks: 0,
        atime: created_time,
        mtime: created_time,
        ctime: created_time,
        crtime: created_time,
        kind: fuser::FileType::Symlink,
        perm: 0o755,
        nlink: 2,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: BLKSIZE,
        flags: 0,
    }
}
