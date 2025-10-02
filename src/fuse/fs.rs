// mapache is a secure, de-duplicating, incremental backup tool.
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use fuser::{
    FUSE_ROOT_ID, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyData, ReplyDirectory,
    ReplyEntry, ReplyOpen, Request,
};

use crate::{
    fuse::stash::{Stash, TTL},
    global::ID,
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotStreamer},
    },
    ui, utils,
};

pub(super) type Inode = u64;

/// A virtual filesystem that uses FUSE to mount the repository snapshots
/// in a mountpoint in the host OS.
pub struct MapacheFS {
    repo: Arc<Repository>,
    stash: Stash,
}

impl MapacheFS {
    /// Mounts a `Repository` in `mountpoint`
    pub unsafe fn mount(repo: Arc<Repository>, mountpoint: &Path, allow_other: bool) -> Result<()> {
        let filesystem = Self {
            repo: repo.clone(),
            stash: Stash::new_root(repo.clone())?,
        };

        let mut mount_options: Vec<MountOption> =
            vec![MountOption::RO, MountOption::DefaultPermissions];
        if allow_other {
            mount_options.push(MountOption::AllowOther);
        }

        if let Err(e) = fuser::mount2(filesystem, mountpoint, &mount_options) {
            ui::cli::error!("FUSE error: {}", e.to_string());
            ui::cli::log!("Unmounting...");
            Self::unmount(mountpoint).with_context(|| "Failed to unmount after error.")?;
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
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), libc::c_int> {
        let snapshot_streamer = SnapshotStreamer::new(self.repo.clone());
        if let Err(e) = &snapshot_streamer {
            ui::cli::error!("Failed to read snapshots: {}", e.to_string());
        }
        let mut snapshots: Vec<(ID, Snapshot)> = snapshot_streamer.unwrap().collect();
        snapshots.sort_unstable_by_key(|(_, snapshot)| snapshot.timestamp);

        // snapshots
        let snapshots_ino = self.stash.add_dir(FUSE_ROOT_ID, String::from("snapshots"));

        // ids
        let ids_ino = self.stash.add_dir(snapshots_ino, String::from("ids"));

        for (id, snapshot) in &snapshots {
            self.stash
                .add_snapshot_dir(ids_ino, id.to_hex(), snapshot.tree.clone());
        }

        // by_date
        let by_date_ino = self.stash.add_dir(snapshots_ino, String::from("by_date"));
        for (id, snapshot) in &snapshots {
            let name = format!(
                "{} - {}",
                utils::pretty_print_timestamp(&snapshot.timestamp),
                id.to_short_hex(4)
            );
            let target = format!("../ids/{}", id.to_hex());
            self.stash.add_symlink(by_date_ino, name.clone(), target);
        }

        // Links to the latest snapshot
        if !snapshots.is_empty() {
            let (latest_id, latest_snapshot) = snapshots.last().unwrap().clone();

            self.stash
                .add_symlink(ids_ino, String::from("latest"), latest_id.to_hex());

            let by_date_name = format!(
                "{} - {}",
                utils::pretty_print_timestamp(&latest_snapshot.timestamp),
                latest_id.to_short_hex(4)
            );
            self.stash
                .add_symlink(by_date_ino, String::from("latest"), by_date_name);
        }

        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self
            .stash
            .lookup(parent, name.to_string_lossy().to_string())
        {
            None => {
                reply.error(libc::ENOENT);
            }
            Some(attr) => reply.entry(&TTL, attr, 0),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.stash.get_attr(ino) {
            None => reply.error(libc::ENOENT),
            Some(attr) => reply.attr(&TTL, &attr),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let entries = self.stash.read_dir(ino, offset);

        for (i, (child_ino, file_type, name)) in entries.into_iter().enumerate() {
            let next_offset = offset + (i as i64) + 1;
            if reply.add(child_ino, next_offset, file_type, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.stash.get_attr(ino) {
            Some(attr) if attr.kind == fuser::FileType::RegularFile => {
                // For a read-only filesystem, we don't need a file handle (fh).
                // The kernel typically uses the 'ino' directly for read operations
                // However, FUSE expects a non-zero fh if you intend to use it later.
                // Since we're not maintaining per-file state, we can just return 0
                // but for correctness in case FUSE expects it, using the ino itself
                // as the fh is a common simple approach.
                reply.opened(ino, 0);
            }
            Some(_) => {
                reply.error(libc::EACCES); // Permission denied
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.stash.read_from_file(ino, offset, size) {
            Ok(Some(data)) => reply.data(&data),
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => {
                ui::cli::error!("Failed to read data for ino {}: {}", ino, e.to_string());
                reply.error(libc::EIO);
            }
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.stash.read_link(ino) {
            Err(_) => reply.error(libc::ENOENT),
            Ok(target) => reply.data(target.as_bytes()),
        }
    }
}
