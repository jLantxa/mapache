use std::{ffi::OsStr, path::Path, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use colored::Colorize;
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
    mapache::{ID, traits::BlobLoader},
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotStream},
    },
    ui, utils,
};

/// A virtual filesystem that uses FUSE to mount the repository snapshots
/// in a mountpoint in the host OS.
pub struct MapacheFS<L: BlobLoader + ?Sized> {
    repo: Option<Arc<Repository>>, // Optional, for repo-specific features like snapshots
    stash: RwLock<Stash>,
    tree_cache: TreeCache<L>,
    blob_cache: BlobCache<L>,
    metadata_only: bool, // Do not load file contents.
    rt_handle: tokio::runtime::Handle,
    archive_root_tree: Option<ID>, // For archive mounting
}

pub struct MountOptions {
    pub allow_other: bool,
    pub metadata_only: bool,
    pub data_cache_size: u64,
    pub created_time: chrono::DateTime<chrono::Local>,
}

impl MapacheFS<dyn BlobLoader> {
    /// Mounts a `Repository` in `mountpoint`
    pub fn mount(
        repo: Arc<Repository>,
        mountpoint: &Path,
        allow_other: bool,
        metadata_only: bool,
        data_cache_size: u64,
    ) -> Result<()> {
        tracing::info!(target: "fuse", "Mounting repository at {:?}", mountpoint);
        let created_time = repo.manifest().created_time();

        Self::mount_loader_generic(
            repo.clone(),
            Some(repo),
            None,
            mountpoint,
            MountOptions {
                allow_other,
                metadata_only,
                data_cache_size,
                created_time,
            },
        )
    }

    /// Mounts a `BlobLoader` in `mountpoint`
    pub fn mount_loader(
        loader: Arc<dyn BlobLoader>,
        repo: Option<Arc<Repository>>,
        archive_root_tree: Option<ID>,
        mountpoint: &Path,
        options: MountOptions,
    ) -> Result<()> {
        tracing::info!(target: "fuse", "Mounting loader at {:?}", mountpoint);
        Self::mount_loader_generic(loader, repo, archive_root_tree, mountpoint, options)
    }

    fn mount_loader_generic<L>(
        loader: Arc<L>,
        repo: Option<Arc<Repository>>,
        archive_root_tree: Option<ID>,
        mountpoint: &Path,
        options: MountOptions,
    ) -> Result<()>
    where
        L: BlobLoader + ?Sized + 'static,
    {
        let stash = Stash::new(options.created_time.into());
        let rt_handle = tokio::runtime::Handle::current();

        let tree_cache = TreeCache::new(loader.clone(), 512);
        let blob_cache = BlobCache::new(loader.clone(), options.data_cache_size);

        let filesystem: MapacheFS<L> = MapacheFS {
            repo,
            stash: RwLock::new(stash),
            tree_cache,
            blob_cache,
            metadata_only: options.metadata_only,
            rt_handle,
            archive_root_tree,
        };

        let mut config = Config::default();
        config.mount_options = vec![
            MountOption::RO,
            MountOption::CUSTOM("nodev".to_string()),
            MountOption::CUSTOM("nosuid".to_string()),
            MountOption::CUSTOM("noexec".to_string()),
        ];
        config.acl = if options.allow_other {
            SessionACL::All
        } else {
            SessionACL::Owner
        };
        config.n_threads = Some(1);
        config.clone_fd = false;

        tracing::debug!(target: "fuse", "Starting FUSE session at {:?}", mountpoint);
        if let Err(e) = fuser::mount2(filesystem, mountpoint, &config) {
            Self::unmount(mountpoint).context("Failed to unmount after error.")?;
            return Err(anyhow!("FUSE error: {}", e));
        }

        Ok(())
    }

    /// Unmounts the filesystem from `mountpoint`
    pub fn unmount(mountpoint: &Path) -> Result<()> {
        tracing::info!(target: "fuse", "Unmounting {:?}", mountpoint);
        #[cfg(target_os = "linux")]
        let mut cmd = std::process::Command::new("/usr/bin/fusermount");
        #[cfg(target_os = "linux")]
        cmd.arg("-u");

        #[cfg(target_os = "macos")]
        let mut cmd = std::process::Command::new("/usr/sbin/umount");

        cmd.arg(mountpoint)
            .output()
            .map_err(|_| anyhow!("Failed to unmount {}", mountpoint.display()))?;

        Ok(())
    }
}

impl<L: BlobLoader + ?Sized> MapacheFS<L> {
    async fn ensure_loaded(&self, ino: INodeNo) -> Result<()> {
        let (tree_id, parent_crtime) = {
            let stash = self.stash.read();
            let node = match stash.get_node(ino) {
                Some(n) => n,
                None => return Ok(()),
            };
            match &node.kind {
                NodeKind::LazyDir { tree_id } => (*tree_id, node.attr.crtime),
                _ => return Ok(()),
            }
        };

        let tree = self.tree_cache.load(&tree_id).await?;

        let mut children_nodes = Vec::new();
        {
            let mut stash = self.stash.write();
            // Check again after lock
            if let Some(node) = stash.get_node(ino)
                && let NodeKind::LazyDir { .. } = &node.kind
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
                                .as_ref()
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

impl<L: BlobLoader + ?Sized + 'static> Filesystem for MapacheFS<L> {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), std::io::Error> {
        tracing::debug!(target: "fuse", "FUSE filesystem initialized");
        self.rt_handle.block_on(async {
            if let Some(root_tree_id) = self.archive_root_tree {
                // For archives, we mount the tree directly at the root
                tracing::debug!(target: "fuse", "Mounting archive root tree: {}", root_tree_id.to_short_hex(8));
                let mut stash = self.stash.write();
                stash.upgrade_root_to_lazy_dir(root_tree_id);
                return;
            }

            // For repositories, we use the standard snapshots structure
            if let Some(repo) = &self.repo {
                tracing::debug!(target: "fuse", "Loading snapshots for FUSE mount");
                const DATE_FORMAT_STR: &str = "%Y-%m-%d %H:%M:%S %:z";
                let mut snapshots: Vec<(ID, Snapshot)> = Vec::new();
                match SnapshotStream::new(repo.clone()).await {
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
                        utils::pretty_print_timestamp(&snapshot.timestamp, Some(DATE_FORMAT_STR)),
                        id.to_short_hex(4)
                    );
                    let target = format!("../ids/{}", id.to_hex());
                    stash.add_symlink(by_date_ino, name.clone(), target);
                }

                if !snapshots.is_empty() {
                    let (latest_id, latest_snapshot) = snapshots.last().unwrap().clone();
                    stash.add_symlink(ids_ino, String::from("latest"), latest_id.to_hex());
                    let by_date_name = format!(
                        "{} - {}",
                        utils::pretty_print_timestamp(
                            &latest_snapshot.timestamp,
                            Some(DATE_FORMAT_STR)
                        ),
                        latest_id.to_short_hex(4)
                    );
                    stash.add_symlink(by_date_ino, String::from("latest"), by_date_name);
                }
            }
        });

        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy().to_string();
        tracing::debug!(target: "fuse", "LOOKUP parent={}, name={}", parent.0, name_str);

        if let Some(attr) = self.stash.read().get_attr_by_name(parent, &name_str) {
            return reply.entry(&TTL, &attr, Generation(0));
        }

        let result = self.rt_handle.block_on(async {
            self.ensure_loaded(parent).await?;
            Ok::<_, anyhow::Error>(self.stash.read().get_attr_by_name(parent, &name_str))
        });

        match result {
            Ok(Some(attr)) => reply.entry(&TTL, &attr, Generation(0)),
            _ => {
                tracing::debug!(target: "fuse", "LOOKUP failed: not found");
                reply.error(Errno::ENOENT)
            }
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        tracing::trace!(target: "fuse", "GETATTR ino={}", ino.0);
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
        tracing::debug!(target: "fuse", "READDIR ino={}, offset={}", ino.0, offset);
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
        tracing::debug!(target: "fuse", "OPEN ino={}", ino.0);
        match self.stash.read().get_attr(ino) {
            Some(attr) if attr.kind == fuser::FileType::RegularFile => {
                if self.metadata_only {
                    reply.error(Errno::EACCES);
                } else {
                    reply.opened(FileHandle(ino.into()), FopenFlags::empty());
                }
            }
            Some(_) => reply.error(Errno::EACCES),
            None => reply.error(Errno::ENOENT),
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
        tracing::trace!(target: "fuse", "READ ino={}, offset={}, size={}", ino.0, offset, size);
        if self.metadata_only {
            return reply.error(Errno::EACCES);
        }

        let result = self.rt_handle.block_on(async {
            let blobs = {
                let stash = self.stash.read();
                match stash.get_node(ino) {
                    Some(n) => match &n.kind {
                        NodeKind::File { blobs } => blobs.clone(),
                        _ => bail!("Not a file"),
                    },
                    None => bail!("Not found"),
                }
            };

            let mut buffer = Vec::with_capacity(size as usize);
            let mut file_pos: u64 = 0;
            let mut remaining_size = size;
            let mut current_offset = offset;

            for blob_id in &blobs {
                if remaining_size == 0 {
                    break;
                }

                let blob_data = self.blob_cache.load(blob_id).await?;
                let blob_len = blob_data.len() as u64;
                let blob_end = file_pos + blob_len;

                if blob_end <= current_offset {
                    file_pos += blob_len;
                    continue;
                }

                let start_in_blob = current_offset.saturating_sub(file_pos) as usize;
                let bytes_available = (blob_len as usize).saturating_sub(start_in_blob);
                let bytes_to_read = bytes_available.min(remaining_size as usize);

                if bytes_to_read > 0 {
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
        tracing::debug!(target: "fuse", "READLINK ino={}", ino.0);
        match self.stash.read().read_link(ino) {
            Some(target) => reply.data(target.as_bytes()),
            None => reply.error(Errno::ENOENT),
        };
    }
}
