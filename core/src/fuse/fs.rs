use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use fuser::{
    Config, Errno, FileHandle, Filesystem, FopenFlags, Generation, INodeNo, KernelConfig,
    LockOwner, MountOption, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen,
    Request, SessionACL,
};
use parking_lot::Mutex;

use crate::{
    fuse::stash::{Stash, TTL},
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
    stash: Mutex<Stash>,
    metadata_only: bool, // Do not load file contents.
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
        let stash = Stash::new_root(repo.clone(), data_cache_size)?;

        let filesystem = Self {
            repo: repo.clone(),
            stash: Mutex::new(stash),
            metadata_only,
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
            ui::cli::error!("FUSE error: {}", e.to_string());
            Self::unmount(mountpoint).context("Failed to unmount after error.")?;
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
}

impl Filesystem for MapacheFS {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), std::io::Error> {
        let snapshot_stream = SnapshotStream::new(self.repo.clone());
        if let Err(e) = &snapshot_stream {
            ui::cli::error!("Failed to read snapshots: {}", e.to_string());
        }
        let mut snapshots: Vec<(ID, Snapshot)> = snapshot_stream.unwrap().collect();
        snapshots.sort_unstable_by_key(|(_, snapshot)| snapshot.timestamp);

        // snapshots
        let snapshots_ino = self
            .stash
            .lock()
            .add_dir(INodeNo::ROOT, String::from("snapshots"));

        // ids
        let ids_ino = self
            .stash
            .lock()
            .add_dir(snapshots_ino, String::from("ids"));

        for (id, snapshot) in &snapshots {
            self.stash
                .lock()
                .add_snapshot_dir(ids_ino, id.to_hex(), snapshot.tree);
        }

        // by_date
        let by_date_ino = self
            .stash
            .lock()
            .add_dir(snapshots_ino, String::from("by_date"));
        for (id, snapshot) in &snapshots {
            let name = format!(
                "{} - {}",
                utils::pretty_print_timestamp(&snapshot.timestamp),
                id.to_short_hex(4)
            );
            let target = format!("../ids/{}", id.to_hex());
            self.stash
                .lock()
                .add_symlink(by_date_ino, name.clone(), target);
        }

        // Links to the latest snapshot
        if !snapshots.is_empty() {
            let (latest_id, latest_snapshot) = snapshots.last().unwrap().clone();

            self.stash
                .lock()
                .add_symlink(ids_ino, String::from("latest"), latest_id.to_hex());

            let by_date_name = format!(
                "{} - {}",
                utils::pretty_print_timestamp(&latest_snapshot.timestamp),
                latest_id.to_short_hex(4)
            );
            self.stash
                .lock()
                .add_symlink(by_date_ino, String::from("latest"), by_date_name);
        }

        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        match self
            .stash
            .lock()
            .lookup(parent, name.to_string_lossy().to_string())
        {
            None => {
                reply.error(Errno::ENOENT);
            }
            Some(attr) => reply.entry(&TTL, attr, Generation(0)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        match self.stash.lock().get_attr(ino) {
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
        let entries = self.stash.lock().read_dir(ino, offset);

        for (i, (child_ino, file_type, name)) in entries.into_iter().enumerate() {
            let next_offset = offset + (i as u64) + 1;
            if reply.add(child_ino, next_offset, file_type, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        match self.stash.lock().get_attr(ino) {
            Some(attr) if attr.kind == fuser::FileType::RegularFile => {
                // For a read-only filesystem, we don't need a file handle (fh).
                // The kernel typically uses the 'ino' directly for read operations
                // However, FUSE expects a non-zero fh if you intend to use it later.
                // Since we're not maintaining per-file state, we can just return 0
                // but for correctness in case FUSE expects it, using the ino itself
                // as the fh is a common simple approach.

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

        match self.stash.lock().read_from_file(ino, offset, size) {
            Ok(Some(data)) => reply.data(&data),
            Ok(None) => reply.error(Errno::ENOENT),
            Err(e) => {
                ui::cli::error!("Failed to read data for ino {}: {}", ino, e.to_string());
                reply.error(Errno::EIO);
            }
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        match self.stash.lock().read_link(ino) {
            Err(_) => reply.error(Errno::ENOENT),
            Ok(target) => reply.data(target.as_bytes()),
        };
    }
}
