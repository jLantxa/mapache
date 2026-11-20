use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

use fuser::{FileAttr, INodeNo};

use crate::{
    common::ID,
    fs::node::{Node, NodeType},
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
        if name == "." {
            return self.get_attr(parent);
        }
        if name == ".." {
            let parent_node = self.nodes.get(&parent)?;
            return self.get_attr(parent_node.parent);
        }
        if let Some(&ino) = self.path_cache.get(&(parent, name.to_string())) {
            return self.get_attr(ino);
        }
        None
    }

    pub(super) fn get_attr(&self, ino: INodeNo) -> Option<FileAttr> {
        self.nodes.get(&ino).map(|n| n.attr)
    }

    pub(super) fn get_node(&self, ino: INodeNo) -> Option<&FsNode> {
        self.nodes.get(&ino)
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

    pub(super) fn upgrade_root_to_lazy_dir(&mut self, tree_id: ID) {
        if let Some(node) = self.nodes.get_mut(&INodeNo::ROOT) {
            node.kind = NodeKind::LazyDir { tree_id };
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
        perm: (node.metadata.mode.unwrap_or(0o755) & 0o7777) as u16,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn attr(ino: INodeNo, kind: fuser::FileType) -> FileAttr {
        simple_attr(ino, kind, 0, SystemTime::UNIX_EPOCH)
    }

    #[test]
    fn stash_tracks_paths_dot_entries_and_symlinks() {
        let mut stash = Stash::new(SystemTime::UNIX_EPOCH);
        let dir = stash.add_dir(INodeNo::ROOT, "dir".to_string());
        let lazy = stash.add_snapshot_dir(dir, "snap".to_string(), ID::from_content(b"tree"));
        let link = stash.add_symlink(INodeNo::ROOT, "link".to_string(), "target".to_string());

        assert_ne!(dir, INodeNo::ROOT);
        assert_ne!(lazy, dir);
        assert_ne!(link, lazy);

        assert_eq!(
            stash.get_attr_by_name(INodeNo::ROOT, "."),
            stash.get_attr(INodeNo::ROOT)
        );
        assert_eq!(
            stash.get_attr_by_name(dir, ".."),
            stash.get_attr(INodeNo::ROOT)
        );
        assert_eq!(
            stash.get_attr_by_name(INodeNo::ROOT, "dir").map(|a| a.kind),
            Some(fuser::FileType::Directory)
        );
        assert_eq!(stash.get_attr_by_name(INodeNo::ROOT, "missing"), None);
        assert_eq!(
            stash.get_attr_by_name(dir, "snap").map(|a| a.ino),
            Some(lazy)
        );
        assert_eq!(stash.read_link(link).as_deref(), Some("target"));
        assert_eq!(stash.read_link(dir), None);

        let names: Vec<_> = stash
            .read_dir(INodeNo::ROOT, 0)
            .into_iter()
            .map(|(_, _, name)| name)
            .collect();
        assert_eq!(names, [".", "..", "dir", "link"]);

        let names: Vec<_> = stash
            .read_dir(INodeNo::ROOT, 2)
            .into_iter()
            .map(|(_, _, name)| name)
            .collect();
        assert_eq!(names, ["dir", "link"]);

        match &stash.get_node(lazy).unwrap().kind {
            NodeKind::LazyDir { .. } => {}
            other => panic!("expected a lazy directory, got {other:?}"),
        }
    }

    #[test]
    fn stash_upgrade_lazy_dir_indexes_children() {
        let mut stash = Stash::new(SystemTime::UNIX_EPOCH);
        let lazy =
            stash.add_snapshot_dir(INodeNo::ROOT, "snap".to_string(), ID::from_content(b"tree"));
        let ino_a = stash.next_ino();
        let ino_b = stash.next_ino();
        let blob_id = ID::from_content(b"blob");

        let mut attr_a = attr(ino_a, fuser::FileType::RegularFile);
        attr_a.size = 7;
        let attr_b = attr(ino_b, fuser::FileType::Symlink);

        stash.upgrade_lazy_dir(
            lazy,
            vec![
                (
                    "a".to_string(),
                    NodeKind::File {
                        blobs: vec![blob_id],
                    },
                    attr_a,
                ),
                (
                    "b".to_string(),
                    NodeKind::Symlink {
                        target: "a".to_string(),
                    },
                    attr_b,
                ),
            ],
        );

        assert_eq!(stash.get_attr_by_name(lazy, "a").map(|a| a.size), Some(7));
        assert_eq!(
            stash.get_attr_by_name(lazy, "b").map(|a| a.kind),
            Some(fuser::FileType::Symlink)
        );
        assert_eq!(stash.get_node(lazy).unwrap().attr.nlink, 4);

        let names: Vec<_> = stash
            .read_dir(lazy, 0)
            .into_iter()
            .map(|(_, _, name)| name)
            .collect();
        assert_eq!(names, [".", "..", "a", "b"]);

        assert_eq!(stash.get_node(ino_a).unwrap().parent, lazy);
        assert_eq!(stash.read_link(ino_b).as_deref(), Some("a"));
        match &stash.get_node(ino_a).unwrap().kind {
            NodeKind::File { blobs } => assert_eq!(blobs, &[blob_id]),
            other => panic!("expected a file, got {other:?}"),
        }
    }

    #[test]
    fn node_to_fileattr_maps_metadata() {
        let parent_time = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(200);

        let mut node = Node {
            name: "n".to_string(),
            metadata: crate::fs::node::Metadata {
                size: 123,
                mode: Some(0o104_755),
                owner_uid: Some(1000),
                owner_gid: Some(1001),
                rdev: Some(42),
                modified_time: Some(modified),
                ..Default::default()
            },
            ..Default::default()
        };

        for (node_type, expected_kind) in [
            (NodeType::File, fuser::FileType::RegularFile),
            (NodeType::Directory, fuser::FileType::Directory),
            (NodeType::Symlink, fuser::FileType::Symlink),
            (NodeType::BlockDevice, fuser::FileType::BlockDevice),
            (NodeType::CharDevice, fuser::FileType::CharDevice),
            (NodeType::Fifo, fuser::FileType::NamedPipe),
            (NodeType::Socket, fuser::FileType::Socket),
        ] {
            node.node_type = node_type;
            let actual = node_to_fileattr(INodeNo(9), parent_time, &node);
            assert_eq!(actual.kind, expected_kind);
            assert_eq!(actual.perm, 0o4755, "file-type bits must be masked out");
            assert_eq!(actual.uid, 1000);
            assert_eq!(actual.gid, 1001);
            assert_eq!(actual.rdev, 42);
            assert_eq!(actual.size, 123);
            assert_eq!(actual.blocks, 123u64.div_ceil(BLKSIZE as u64));
            assert_eq!(actual.mtime, modified);
            assert_eq!(
                actual.atime, parent_time,
                "missing atime falls back to parent"
            );
        }
    }
}
