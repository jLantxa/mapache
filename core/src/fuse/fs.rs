use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use fuser::{
    Config, Errno, FileHandle, Filesystem, FopenFlags, Generation, INodeNo, KernelConfig,
    LockOwner, MountOption, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen,
    Request, SessionACL,
};
use futures::StreamExt;
use parking_lot::RwLock;

use crate::{
    fs::node::NodeType,
    fuse::cache::{BlobCache, TreeCache},
    fuse::stash::{NodeKind, Stash, TTL, node_to_fileattr},
    mapache::ID,
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotStream},
    },
    ui, utils,
};

/// A virtual filesystem that uses FUSE to mount the repository snapshots
/// in a mountpoint in the host OS.
pub struct MapacheFS {
    repo: Arc<Repository>,
    stash: RwLock<Stash>,
    tree_cache: TreeCache,
    blob_cache: BlobCache,
    metadata_only: bool, // Do not load file contents.
    rt_handle: tokio::runtime::Handle,
}

impl MapacheFS {
    /// Mounts a `Repository` in `mountpoint`
    pub fn mount(
        repo: Arc<Repository>,
        mountpoint: &Path,
        allow_other: bool,
        metadata_only: bool,
        data_cache_size: u64,
    ) -> Result<()> {
        let stash = Stash::new(repo.manifest().created_time().into());
        let rt_handle = tokio::runtime::Handle::current();

        let tree_cache = TreeCache::new(repo.clone(), 512);
        let blob_cache = BlobCache::new(repo.clone(), data_cache_size);

        let filesystem = Self {
            repo: repo.clone(),
            stash: RwLock::new(stash),
            tree_cache,
            blob_cache,
            metadata_only,
            rt_handle,
        };

        let mut config = Config::default();
        config.mount_options = vec![MountOption::RO, MountOption::DefaultPermissions];
        config.acl = if allow_other {
            SessionACL::All
        } else {
            SessionACL::Owner
        };
        config.n_threads = None;
        config.clone_fd = false;

        if let Err(e) = fuser::mount2(filesystem, mountpoint, &config) {
            Self::unmount(mountpoint).context("Failed to unmount after error.")?;
            return Err(anyhow!("FUSE error: {}", e));
        }

        Ok(())
    }

    /// Unmounts the filesystem from `mountpoint`
    pub fn unmount(mountpoint: &Path) -> Result<()> {
        std::process::Command::new("fusermount")
            .arg("-u")
            .arg(mountpoint)
            .output()
            .map_err(|_| anyhow!("Failed to unmount {}", mountpoint.display()))?;

        Ok(())
    }

    async fn ensure_loaded(&self, ino: INodeNo) -> Result<()> {
        let (tree_id, parent_crtime) = {
            let stash = self.stash.read();
            let node = match stash.get_node(ino) {
                Some(n) => n,
                None => return Ok(()),
            };
            match node.kind {
                NodeKind::LazyDir { tree_id } => (tree_id, node.attr.crtime),
                _ => return Ok(()),
            }
        };

        let tree = self.tree_cache.load(&tree_id).await?;

        let mut children_nodes = Vec::new();
        {
            let mut stash = self.stash.write();
            // Check again after lock
            if let Some(node) = stash.get_node(ino)
                && let NodeKind::LazyDir { .. } = node.kind
            {
                for n in &tree.nodes {
                    let child_ino = stash.next_ino();
                    let attr = node_to_fileattr(child_ino, parent_crtime, n);
                    let kind = match n.node_type {
                        NodeType::Directory => NodeKind::LazyDir {
                            tree_id: n.tree.unwrap(),
                        },
                        NodeType::Symlink => NodeKind::Symlink {
                            target: n
                                .symlink_info
                                .clone()
                                .map(|i| i.target_path.to_string_lossy().to_string())
                                .unwrap_or_default(),
                        },
                        _ => NodeKind::File {
                            blobs: n.blobs.clone().unwrap_or_default(),
                        },
                    };
                    children_nodes.push((n.name.clone(), kind, attr));
                }
                stash.upgrade_lazy_dir(ino, children_nodes);
            }
        }
        Ok(())
    }
}

impl Filesystem for MapacheFS {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), std::io::Error> {
        self.rt_handle.block_on(async {
            let mut snapshots: Vec<(ID, Snapshot)> = Vec::new();
            match SnapshotStream::new(self.repo.clone()).await {
                Ok(mut stream) => {
                    while let Some(res) = stream.next().await {
                        match res {
                            Ok(pair) => snapshots.push(pair),
                            Err(e) => ui::cli::error!("Failed to load snapshot: {}", e),
                        }
                    }
                }
                Err(e) => {
                    ui::cli::error!("Failed to read snapshots: {}", e.to_string());
                }
            };
            snapshots.sort_unstable_by_key(|(_, snapshot)| snapshot.timestamp);

            let mut stash = self.stash.write();

            // snapshots
            let snapshots_ino = stash.add_dir(INodeNo::ROOT, String::from("snapshots"));

            // ids
            let ids_ino = stash.add_dir(snapshots_ino, String::from("ids"));

            for (id, snapshot) in &snapshots {
                stash.add_snapshot_dir(ids_ino, id.to_hex(), snapshot.tree);
            }

            // by_date
            let by_date_ino = stash.add_dir(snapshots_ino, String::from("by_date"));
            for (id, snapshot) in &snapshots {
                let name = format!(
                    "{} - {}",
                    utils::pretty_print_timestamp(&snapshot.timestamp),
                    id.to_short_hex(4)
                );
                let target = format!("../ids/{}", id.to_hex());
                stash.add_symlink(by_date_ino, name.clone(), target);
            }

            // Links to the latest snapshot
            if !snapshots.is_empty() {
                let (latest_id, latest_snapshot) = snapshots.last().unwrap().clone();

                stash.add_symlink(ids_ino, String::from("latest"), latest_id.to_hex());

                let by_date_name = format!(
                    "{} - {}",
                    utils::pretty_print_timestamp(&latest_snapshot.timestamp),
                    latest_id.to_short_hex(4)
                );
                stash.add_symlink(by_date_ino, String::from("latest"), by_date_name);
            }
        });

        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy().to_string();

        // Quick check
        if let Some(attr) = self.stash.read().get_attr_by_name(parent, &name_str) {
            return reply.entry(&TTL, &attr, Generation(0));
        }

        // If not found, load the directory.
        let result = self.rt_handle.block_on(async {
            self.ensure_loaded(parent).await?;
            Ok::<_, anyhow::Error>(self.stash.read().get_attr_by_name(parent, &name_str))
        });

        match result {
            Ok(Some(attr)) => reply.entry(&TTL, &attr, Generation(0)),
            _ => reply.error(Errno::ENOENT),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.stash.read().get_attr(ino) {
            None => reply.error(Errno::ENOENT),
            Some(attr) => reply.attr(&TTL, &attr),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let entries = self.rt_handle.block_on(async {
            let _ = self.ensure_loaded(ino).await;
            self.stash.read().read_dir(ino, offset)
        });

        for (i, (child_ino, file_type, name)) in entries.into_iter().enumerate() {
            let next_offset = offset + (i as u64) + 1;
            if reply.add(child_ino, next_offset, file_type, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match self.stash.read().get_attr(ino) {
            Some(attr) if attr.kind == fuser::FileType::RegularFile => {
                if self.metadata_only {
                    reply.error(Errno::EACCES); // Permission denied
                } else {
                    reply.opened(FileHandle(ino.into()), FopenFlags::empty());
                }
            }
            Some(_) => {
                reply.error(Errno::EACCES); // Permission denied
            }
            None => {
                reply.error(Errno::ENOENT);
            }
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        if self.metadata_only {
            return reply.error(Errno::EACCES);
        }

        let result = self.rt_handle.block_on(async {
            let blobs = {
                let stash = self.stash.read();
                match stash.get_node(ino) {
                    Some(n) => match n.kind {
                        NodeKind::File { blobs } => blobs,
                        _ => bail!("Not a file"),
                    },
                    None => bail!("Not found"),
                }
            };

            let index = self.repo.index();
            let mut buffer = Vec::with_capacity(size as usize);
            let mut file_pos: u64 = 0;
            let mut remaining_size = size;
            let mut current_offset = offset;

            for blob_id in &blobs {
                if remaining_size == 0 {
                    break;
                }

                let descriptor = index
                    .get(blob_id)
                    .ok_or_else(|| anyhow!("Missing blob descriptor for {blob_id}"))?;
                let blob_len = descriptor.raw_length as u64;
                let blob_end = file_pos + blob_len;

                if blob_end <= current_offset {
                    file_pos += blob_len;
                    continue;
                }

                let start_in_blob = current_offset.saturating_sub(file_pos) as usize;
                let bytes_available = (blob_len as usize).saturating_sub(start_in_blob);
                let bytes_to_read = bytes_available.min(remaining_size as usize);

                if bytes_to_read > 0 {
                    let blob_data = self.blob_cache.load(blob_id).await?;
                    buffer.extend_from_slice(
                        &blob_data[start_in_blob..start_in_blob + bytes_to_read],
                    );

                    remaining_size -= bytes_to_read as u32;
                    current_offset += bytes_to_read as u64;
                }

                file_pos += blob_len;
            }

            Ok(buffer)
        });

        match result {
            Ok(data) => reply.data(&data),
            Err(e) => {
                if e.to_string() == "Not found" {
                    reply.error(Errno::ENOENT);
                } else {
                    ui::cli::error!("Failed to read data for ino {}: {}", ino, e.to_string());
                    reply.error(Errno::EIO);
                }
            }
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.stash.read().read_link(ino) {
            Some(target) => reply.data(target.as_bytes()),
            None => reply.error(Errno::ENOENT),
        };
    }
}
