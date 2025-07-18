// mapache is an incremental backup tool
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
use colored::Colorize;
use ctrlc;
use fuser::{
    Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, Request,
};
use libc;

use crate::{
    fuse::stash::{ROOT_INODE, Stash, TTL},
    global::ID,
    repository::{
        RepositoryBackend,
        snapshot::{Snapshot, SnapshotStreamer},
    },
    ui, utils,
};

pub(super) type Inode = u64;

/// A virtual filesystem that uses FUSE to mount the repository snapshots
/// in a mountpoint in the host OS.
pub struct MapacheFS {
    repo: Arc<dyn RepositoryBackend>,
    stash: Stash,
}

impl MapacheFS {
    /// Mounts a `RepositoryBackend` in `mountpoint`
    pub unsafe fn mount(
        repo: Arc<dyn RepositoryBackend>,
        mountpoint: &Path,
        allow_other: bool,
    ) -> Result<()> {
        // Listen for CTRL + C to unmount.
        let mpoint = mountpoint.to_path_buf();
        ctrlc::set_handler(move || {
            let _ = Self::unmount(&mpoint);
        })?;

        let filesystem = Self {
            repo: repo.clone(),
            stash: Stash::new_root(repo.clone())?,
        };

        let mut mount_options: Vec<MountOption> = vec![MountOption::RO];
        if allow_other {
            mount_options.push(MountOption::AllowOther);
        }

        ui::cli::log!("Mounting repository in {}", mountpoint.display());
        ui::cli::log!(
            "Press {} to finish or unmount the filesystem manually.",
            "Ctrl+C".bold()
        );

        if let Err(e) = fuser::mount2(filesystem, mountpoint, &mount_options) {
            ui::cli::error!("FUSE error: {}", e.to_string());
            ui::cli::log!("Unmounting...");
            Self::unmount(mountpoint).with_context(|| "Failed to unmount after error.")?;
        }

        Ok(())
    }

    /// Unmounts the filesystem from `mountpoint`
    fn unmount(mountpoint: &Path) -> Result<()> {
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
        let snapshots: Vec<(ID, Snapshot)> = snapshot_streamer.unwrap().collect();
        let (latest_id, latest_snapshot) = snapshots.last().unwrap().clone();

        // snapshots
        let snapshots_ino = self.stash.add_dir(ROOT_INODE, String::from("snapshots"));

        // ids
        let ids_ino = self.stash.add_dir(snapshots_ino, String::from("ids"));

        for (id, snapshot) in &snapshots {
            self.stash
                .add_snapshot_dir(ids_ino, id.to_hex(), snapshot.tree.clone());
        }
        self.stash
            .add_symlink(ids_ino, String::from("latest"), latest_id.to_hex());

        // by_date
        let by_date_ino = self.stash.add_dir(snapshots_ino, String::from("by_date"));
        for (id, snapshot) in &snapshots {
            let name = utils::pretty_print_timestamp(&snapshot.timestamp);
            let target = format!("../ids/{}", id.to_hex());
            self.stash.add_symlink(by_date_ino, name.clone(), target);
        }
        self.stash.add_symlink(
            by_date_ino,
            String::from("latest"),
            utils::pretty_print_timestamp(&latest_snapshot.timestamp),
        );

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

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, _reply: ReplyOpen) {
        // TODO
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        _reply: ReplyData,
    ) {
        // TODO
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.stash.read_link(ino) {
            Err(_) => reply.error(libc::ENOENT),
            Ok(target) => reply.data(target.as_bytes()),
        }
    }
}
