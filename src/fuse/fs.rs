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
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use colored::Colorize;
use ctrlc;
use fuser::{
    Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, Request,
};
use libc;

use crate::{repository::RepositoryBackend, ui};

pub(super) type Inode = u64;

pub(super) const ROOT_INODE: Inode = 1;
pub(super) const BLKSIZE: u32 = 512;
pub(super) const TTL: Duration = Duration::from_secs(60);

/// A virtual filesystem that uses FUSE to mount the repository snapshots
/// in a mountpoint in the host OS.
pub struct MapacheFS {
    repo: Arc<dyn RepositoryBackend>,
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

        let filesystem = Self { repo };

        let mut mount_options: Vec<MountOption> = vec![MountOption::RO];
        if allow_other {
            mount_options.push(MountOption::AllowOther);
        }

        ui::cli::log!("Mounting repo in {}", mountpoint.display());
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
            .arg(&mountpoint)
            .output()
            .map_err(|_| anyhow!("Failed to unmount {}", mountpoint.display()))?;

        Ok(())
    }
}

impl Filesystem for MapacheFS {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), libc::c_int> {
        // TODO: Init bridge tree and snapshot roots
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        // TODO
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        // TODO
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: ReplyDirectory,
    ) {
        // TODO
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        // TODO
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        // TODO
    }
}
