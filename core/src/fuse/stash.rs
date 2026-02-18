use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

use fuser::{FileAttr, INodeNo};

use crate::{
    fs::node::{Node, NodeType},
    mapache::ID,
};

pub(super) const BLKSIZE: u32 = 512;
pub(super) const TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub(super) struct FsNode {
    pub inode: INodeNo,
    pub parent: INodeNo,
    pub attr: FileAttr,
    pub kind: NodeKind,
}

#[derive(Debug, Clone)]
pub(super) enum NodeKind {
    /// A directory we have fully loaded into memory
    Dir { children: BTreeMap<String, INodeNo> },
    /// A directory represented by a Tree ID that we haven't loaded yet
    LazyDir { tree_id: ID },
    /// A standard file with content blobs
    File { blobs: Vec<ID> },
    /// A symlink
    Symlink { target: String },
}

/// A purely synchronous structure that manages the INode mapping and
/// in-memory tree of FUSE nodes.
pub(super) struct Stash {
    ino_counter: u64,
    nodes: BTreeMap<INodeNo, FsNode>,
    path_cache: BTreeMap<(INodeNo, String), INodeNo>,
    created_time: SystemTime,
}

impl Stash {
    pub(super) fn new(created_time: SystemTime) -> Self {
        let root_attr = simple_attr(INodeNo::ROOT, fuser::FileType::Directory, 0, created_time);

        let mut nodes = BTreeMap::new();
        nodes.insert(
            INodeNo::ROOT,
            FsNode {
                inode: INodeNo::ROOT,
                parent: INodeNo::ROOT,
                attr: root_attr,
                kind: NodeKind::Dir {
                    children: BTreeMap::new(),
                },
            },
        );

        Self {
            ino_counter: INodeNo::ROOT.0,
            nodes,
            path_cache: BTreeMap::new(),
            created_time,
        }
    }

    pub(super) fn add_dir(&mut self, parent: INodeNo, name: String) -> INodeNo {
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

    pub(super) fn add_snapshot_dir(
        &mut self,
        parent: INodeNo,
        name: String,
        tree_id: ID,
    ) -> INodeNo {
        let attr = self.create_attr(fuser::FileType::Directory, 0);
        self.insert_entry(parent, name, attr, NodeKind::LazyDir { tree_id })
    }

    pub(super) fn add_symlink(&mut self, parent: INodeNo, name: String, target: String) -> INodeNo {
        let attr = self.create_attr(fuser::FileType::Symlink, target.len() as u64);
        self.insert_entry(parent, name, attr, NodeKind::Symlink { target })
    }

    pub(super) fn get_attr_by_name(&self, parent: INodeNo, name: &str) -> Option<FileAttr> {
        if let Some(&ino) = self.path_cache.get(&(parent, name.to_string())) {
            return self.get_attr(ino);
        }
        None
    }

    pub(super) fn get_attr(&self, ino: INodeNo) -> Option<FileAttr> {
        self.nodes.get(&ino).map(|n| n.attr)
    }

    pub(super) fn get_node(&self, ino: INodeNo) -> Option<FsNode> {
        self.nodes.get(&ino).cloned()
    }

    pub(super) fn read_dir(
        &self,
        ino: INodeNo,
        offset: u64,
    ) -> Vec<(INodeNo, fuser::FileType, String)> {
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
                if let Some(child) = self.nodes.get(&child_ino) {
                    entries.push((child_ino, child.attr.kind, name.clone()));
                }
            }
        }

        entries.into_iter().skip(offset as usize).collect()
    }

    pub(super) fn read_link(&self, ino: INodeNo) -> Option<String> {
        match self.nodes.get(&ino)? {
            FsNode {
                kind: NodeKind::Symlink { target },
                ..
            } => Some(target.clone()),
            _ => None,
        }
    }

    pub(super) fn upgrade_lazy_dir(
        &mut self,
        ino: INodeNo,
        children_nodes: Vec<(String, NodeKind, FileAttr)>,
    ) {
        let mut children_map = BTreeMap::new();

        for (name, kind, attr) in children_nodes {
            let child_ino = attr.ino;
            self.nodes.insert(
                child_ino,
                FsNode {
                    inode: child_ino,
                    parent: ino,
                    attr,
                    kind,
                },
            );

            self.path_cache.insert((ino, name.clone()), child_ino);
            children_map.insert(name, child_ino);
        }

        if let Some(node) = self.nodes.get_mut(&ino) {
            let child_count = children_map.len();
            node.kind = NodeKind::Dir {
                children: children_map,
            };

            if node.attr.kind == fuser::FileType::Directory {
                node.attr.nlink = 2 + child_count as u32;
            }
        }
    }

    fn insert_entry(
        &mut self,
        parent_ino: INodeNo,
        name: String,
        attr: FileAttr,
        kind: NodeKind,
    ) -> INodeNo {
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
        self.ino_counter += 1;
        simple_attr(INodeNo(self.ino_counter), kind, size, self.created_time)
    }

    pub(super) fn next_ino(&mut self) -> INodeNo {
        self.ino_counter += 1;
        INodeNo(self.ino_counter)
    }
}

pub(super) fn simple_attr(
    ino: INodeNo,
    kind: fuser::FileType,
    size: u64,
    time: SystemTime,
) -> FileAttr {
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

pub(super) fn node_to_fileattr(ino: INodeNo, parent_time: SystemTime, node: &Node) -> FileAttr {
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
