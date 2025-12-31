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
};

pub(super) const BLKSIZE: u32 = 512;
pub(super) const TTL: Duration = Duration::from_secs(60);
pub(super) const TREE_CACHE_CAPACITY: usize = 512;

#[derive(Debug)]
pub(super) struct FsNode {
    inode: Inode,
    parent: Inode,
    attr: FileAttr,
    kind: NodeKind,
}

#[derive(Debug)]
pub(super) enum NodeKind {
    /// A directory we have fully loaded into memory
    Dir { children: BTreeMap<String, Inode> },
    /// A directory represented by a Tree ID that we haven't loaded yet
    LazyDir { tree_id: ID },
    /// A standard file with content blobs
    File { blobs: Vec<ID> },
    /// A symlink
    Symlink { target: String },
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
    pub(super) fn new_root(repo: Arc<Repository>, data_cache_size: u64) -> Result<Self> {
        let root_attr = simple_attr(
            FUSE_ROOT_ID,
            fuser::FileType::Directory,
            0,
            repo.manifest().created_time().into(),
        );

        let mut stash = Self {
            repo: repo.clone(),
            ino_counter: FUSE_ROOT_ID,
            nodes: BTreeMap::new(),
            path_cache: BTreeMap::new(),
            tree_cache: TreeCache::new(repo.clone(), TREE_CACHE_CAPACITY),
            blob_cache: BlobCache::new(repo.clone(), data_cache_size),
        };

        // Manually insert root
        stash.nodes.insert(
            FUSE_ROOT_ID,
            FsNode {
                inode: FUSE_ROOT_ID,
                parent: FUSE_ROOT_ID,
                attr: root_attr,
                kind: NodeKind::Dir {
                    children: BTreeMap::new(),
                },
            },
        );

        Ok(stash)
    }

    pub(super) fn add_dir(&mut self, parent: Inode, name: String) -> Inode {
        let attr = self.create_attr(fuser::FileType::Directory, 0);
        self.insert_entry(
            parent,
            name,
            attr,
            NodeKind::Dir {
                children: BTreeMap::new(),
            },
        )
    }

    pub(super) fn add_snapshot_dir(&mut self, parent: Inode, name: String, tree_id: ID) -> Inode {
        let attr = self.create_attr(fuser::FileType::Directory, 0);
        self.insert_entry(parent, name, attr, NodeKind::LazyDir { tree_id })
    }

    pub(super) fn add_symlink(&mut self, parent: Inode, name: String, target: String) -> Inode {
        let attr = self.create_attr(fuser::FileType::Symlink, target.len() as u64);
        self.insert_entry(parent, name, attr, NodeKind::Symlink { target })
    }

    pub(super) fn lookup(&mut self, parent: Inode, name: String) -> Option<&FileAttr> {
        if let Some(&ino) = self.path_cache.get(&(parent, name.clone())) {
            return self.nodes.get(&ino).map(|n| &n.attr);
        }

        if self.ensure_loaded(parent).is_ok()
            && let Some(&ino) = self.path_cache.get(&(parent, name))
        {
            return self.nodes.get(&ino).map(|n| &n.attr);
        }

        None
    }

    pub(super) fn get_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.nodes.get(&ino).map(|n| n.attr)
    }

    pub(super) fn read_dir(
        &mut self,
        ino: Inode,
        offset: i64,
    ) -> Vec<(Inode, fuser::FileType, String)> {
        // Ensure directory content is loaded
        let _ = self.ensure_loaded(ino);

        let node = match self.nodes.get(&ino) {
            Some(n) => n,
            None => return Vec::new(),
        };

        // Always start with . and ..
        let mut entries = vec![
            (node.inode, node.attr.kind, ".".to_string()),
            (node.parent, fuser::FileType::Directory, "..".to_string()),
        ];

        // Append children if it is a Dir
        if let NodeKind::Dir { children } = &node.kind {
            for (name, &child_ino) in children {
                // We use the name from the map key, and lookup the child to get its Kind
                if let Some(child) = self.nodes.get(&child_ino) {
                    entries.push((child_ino, child.attr.kind, name.clone()));
                }
            }
        }

        // Simple skipping based on offset
        entries.into_iter().skip(offset as usize).collect()
    }

    pub(super) fn read_link(&self, ino: Inode) -> Result<String> {
        match self.nodes.get(&ino) {
            Some(FsNode {
                kind: NodeKind::Symlink { target },
                ..
            }) => Ok(target.clone()),
            Some(_) => Err(anyhow!("Inode {ino} is not a symlink")),
            None => Err(anyhow!("Inode {ino} not found")),
        }
    }

    pub(super) fn read_from_file(
        &mut self,
        ino: Inode,
        offset: i64,
        mut size: u32,
    ) -> Result<Option<Vec<u8>>> {
        let blobs = match self.nodes.get(&ino) {
            Some(FsNode {
                kind: NodeKind::File { blobs },
                ..
            }) => blobs,
            Some(_) => bail!("Inode {ino} is not a file"),
            None => return Ok(None),
        };

        let index_lock = self.repo.index();
        let index = index_lock.read();

        let mut buffer = Vec::with_capacity(size as usize);
        let mut file_pos: i64 = 0;

        for blob_id in blobs {
            if size == 0 {
                break;
            }

            let descriptor = index
                .get(blob_id)
                .ok_or_else(|| anyhow!("Missing blob descriptor for {blob_id}"))?;

            let blob_len = descriptor.raw_length as i64;
            let blob_end = file_pos + blob_len;

            // Does the requested read overlap with this blob?
            if blob_end > offset {
                let start_in_blob = (offset - file_pos).max(0) as usize;
                let bytes_to_read = (blob_len as usize - start_in_blob).min(size as usize);

                let blob_data = self.blob_cache.load(blob_id)?;
                buffer.extend_from_slice(&blob_data[start_in_blob..start_in_blob + bytes_to_read]);

                size -= bytes_to_read as u32;
            }

            file_pos += blob_len;
        }

        Ok(Some(buffer))
    }

    /// If a node is a LazyDir, load the tree from the repo, create child nodes,
    /// and upgrade the node to a standard Dir.
    fn ensure_loaded(&mut self, ino: Inode) -> Result<()> {
        let (tree_id, parent_crtime) = match self.nodes.get(&ino) {
            Some(FsNode {
                kind: NodeKind::LazyDir { tree_id },
                attr,
                ..
            }) => (*tree_id, attr.crtime),
            _ => return Ok(()), // Already loaded or not a directory
        };

        let tree = self.tree_cache.load(&tree_id)?.clone();
        let mut children = BTreeMap::new();

        for node in tree.nodes {
            let child_ino = self.next_ino();
            let child_attr = node_to_fileattr(child_ino, parent_crtime, &node);

            let kind = match node.node_type {
                NodeType::Directory => NodeKind::LazyDir {
                    tree_id: node.tree.unwrap(),
                },
                NodeType::Symlink => NodeKind::Symlink {
                    target: node
                        .symlink_info
                        .map(|i| i.target_path.to_string_lossy().to_string())
                        .unwrap_or_default(),
                },
                _ => NodeKind::File {
                    blobs: node.blobs.unwrap_or_default(),
                },
            };

            self.nodes.insert(
                child_ino,
                FsNode {
                    inode: child_ino,
                    parent: ino,
                    attr: child_attr,
                    kind,
                },
            );

            self.path_cache.insert((ino, node.name.clone()), child_ino);
            children.insert(node.name, child_ino);
        }

        if let Some(node) = self.nodes.get_mut(&ino) {
            let child_count = children.len();
            node.kind = NodeKind::Dir { children };

            if node.attr.kind == fuser::FileType::Directory {
                node.attr.nlink = 2 + child_count as u32;
            }
        }

        Ok(())
    }

    fn insert_entry(
        &mut self,
        parent_ino: Inode,
        name: String,
        attr: FileAttr,
        kind: NodeKind,
    ) -> Inode {
        let ino = attr.ino;

        self.nodes.insert(
            ino,
            FsNode {
                inode: ino,
                parent: parent_ino,
                attr,
                kind,
            },
        );

        self.path_cache.insert((parent_ino, name.clone()), ino);

        // Update parent's children list
        if let Some(parent) = self.nodes.get_mut(&parent_ino)
            && let NodeKind::Dir { children } = &mut parent.kind
        {
            children.insert(name, ino);
        }

        ino
    }

    fn create_attr(&mut self, kind: fuser::FileType, size: u64) -> FileAttr {
        let now = self.repo.manifest().created_time().into();
        simple_attr(self.next_ino(), kind, size, now)
    }

    fn next_ino(&mut self) -> Inode {
        self.ino_counter += 1;
        self.ino_counter
    }
}

fn simple_attr(ino: Inode, kind: fuser::FileType, size: u64, time: SystemTime) -> FileAttr {
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(BLKSIZE as u64),
        atime: time,
        mtime: time,
        ctime: time,
        crtime: time,
        kind,
        perm: 0o755,
        nlink: if kind == fuser::FileType::Directory {
            2
        } else {
            1
        },
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
        blksize: BLKSIZE,
    }
}

fn node_to_fileattr(ino: Inode, parent_time: SystemTime, node: &Node) -> FileAttr {
    let kind = match node.node_type {
        NodeType::File => fuser::FileType::RegularFile,
        NodeType::Directory => fuser::FileType::Directory,
        NodeType::Symlink => fuser::FileType::Symlink,
        NodeType::BlockDevice => fuser::FileType::BlockDevice,
        NodeType::CharDevice => fuser::FileType::CharDevice,
        NodeType::Fifo => fuser::FileType::NamedPipe,
        NodeType::Socket => fuser::FileType::Socket,
    };

    let size = node.metadata.size;

    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(BLKSIZE as u64),
        atime: node.metadata.accessed_time.unwrap_or(parent_time),
        mtime: node.metadata.modified_time.unwrap_or(parent_time),
        ctime: node.metadata.created_time.unwrap_or(parent_time),
        crtime: node.metadata.created_time.unwrap_or(parent_time),
        kind,
        perm: node.metadata.mode.unwrap_or(0o755) as u16,
        nlink: if kind == fuser::FileType::Directory {
            2
        } else {
            1
        },
        uid: node.metadata.owner_uid.unwrap_or(0),
        gid: node.metadata.owner_gid.unwrap_or(0),
        rdev: node.metadata.rdev.unwrap_or(0) as u32,
        blksize: BLKSIZE,
        flags: 0,
    }
}
