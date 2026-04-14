//! The node_restorer module provides lower-level functionality for restoring
//! individual nodes (files, directories, symlinks) and their associated metadata.

use std::sync::Arc;
use std::time::SystemTime;

#[cfg(unix)]
use std::{fs::Permissions, os::unix::fs::PermissionsExt};

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

use std::{fs::OpenOptions, io::Write, path::Path};

use {
    anyhow::{Context, Result},
    filetime::{FileTime, set_file_times},
};

use crate::{
    fs::node::{Node, NodeType},
    repository::repo::Repository,
    ui::restore::RestoreProgressReporter,
};

/// Restores a node to the specified destination path.
/// This function does not restore file times for directory nodes. This must be
/// done in a reparate pass.
pub(crate) async fn restore_node_to_path(
    repo: &Repository,
    progress_reporter: Arc<dyn RestoreProgressReporter>,
    node: &Node,
    dst_path: &Path,
    dry_run: bool,
) -> Result<()> {
    match node.node_type {
        NodeType::File => {
            let blocks = node
                .blobs
                .as_ref()
                .context("File Node must have contents (even if empty)")?;

            let dst_file = if !dry_run {
                if let Some(parent) = dst_path.parent() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!(
                            "Could not create parent directories for file {}",
                            dst_path.display()
                        )
                    })?;
                }

                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(dst_path)
                    .with_context(|| format!("Could not create file {}", dst_path.display()))?;

                Some(file)
            } else {
                None
            };

            for (index, blob_id) in blocks.iter().enumerate() {
                let chunk_data = repo.load_blob(blob_id).await.with_context(|| {
                    format!(
                        "Could not load block #{} ({}) for restoring file {}",
                        index + 1,
                        blob_id,
                        dst_path.display()
                    )
                })?;

                let chunk_size = chunk_data.len() as u64;

                if !dry_run {
                    dst_file
                        .as_ref()
                        .expect("Destination file should exist")
                        .write_all(&chunk_data)
                        .with_context(|| {
                            format!(
                                "Could not restore block #{} ({}) to file {}",
                                index + 1,
                                blob_id,
                                dst_path.display()
                            )
                        })?;
                }

                progress_reporter.processed_bytes(chunk_size);
            }

            // Restore metadata after content is written
            if !dry_run {
                try_restore_node_metadata(node, dst_path, progress_reporter.as_ref());
            }
        }

        NodeType::Directory => {
            if !dry_run {
                std::fs::create_dir_all(dst_path).with_context(|| {
                    format!("Could not create directory {}", dst_path.display())
                })?;

                // We don't restore metadata for directories now, as the filetimes
                // will change if we touch any children nodes. We will restore the
                // directory metadata in a second, dedicated bottom-up pass.
            }
        }

        NodeType::Symlink => {
            let symlink_info = node.symlink_info.as_ref();

            // Show a warning if the symlink metadata is missing and return.
            if symlink_info.is_none() {
                progress_reporter.warning(&format!(
                    "Symlink {} does not have a target path",
                    dst_path.display()
                ));
                return Ok(());
            }
            let symlink_info = symlink_info.expect("Symlink info should exist");

            if !dry_run {
                // Create all parent directories before the symlink
                if let Some(parent) = dst_path.parent() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("Could not create parent directories for symlink {dst_path:?}")
                    })?;
                }

                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(&symlink_info.target_path, dst_path)
                        .with_context(|| format!("Could not create symlink {dst_path:?}"))?;
                }
                #[cfg(windows)]
                {
                    match symlink_info.target_type {
                        // Directory symlink
                        Some(NodeType::Directory) => {
                            std::os::windows::fs::symlink_dir(&symlink_info.target_path, dst_path)
                                .with_context(|| {
                                    format!("Could not create directory symlink {dst_path:?}")
                                })?;
                        }

                        // Everything else (not a directory)
                        Some(_) => {
                            std::os::windows::fs::symlink_file(&symlink_info.target_path, dst_path)
                                .with_context(|| {
                                    format!("Could not create file symlink {dst_path:?}")
                                })?;
                        }
                        // No type info. Show warning.
                        None => {
                            progress_reporter.warning(&format!(
                                "Symlink {} has no type info",
                                dst_path.display()
                            ));
                        }
                    }
                }
            }

            // Restore symlink metadata after creation
            if !dry_run {
                try_restore_symlink_metadata(node, dst_path, progress_reporter.as_ref());
            }
        }

        NodeType::BlockDevice => {
            #[cfg(unix)]
            progress_reporter.warning(&format!(
                "Restoration of block device {} not supported yet.",
                dst_path.display()
            ));
            #[cfg(not(unix))]
            progress_reporter.warning(&format!(
                "Block device restoration not supported on this operating system: {}",
                dst_path.display()
            ));
        }

        NodeType::CharDevice => {
            #[cfg(unix)]
            progress_reporter.warning(&format!(
                "Restoration of character device {} not supported yet.",
                dst_path.display()
            ));
            #[cfg(not(unix))]
            progress_reporter.warning(&format!(
                "Character device restoration not supported on this operating system: {}",
                dst_path.display()
            ));
        }

        NodeType::Fifo => {
            #[cfg(unix)]
            progress_reporter.warning(&format!(
                "Restoration of FIFO (named pipe) {} not supported yet.",
                dst_path.display()
            ));
            #[cfg(not(unix))]
            progress_reporter.warning(&format!(
                "FIFO restoration not supported on this operating system: {}",
                dst_path.display()
            ));
        }

        NodeType::Socket => {
            #[cfg(unix)]
            progress_reporter.warning(&format!(
                "Restoration of socket {} not supported yet.",
                dst_path.display()
            ));
            #[cfg(not(unix))]
            progress_reporter.warning(&format!(
                "Socket restoration not supported on this operating system: {}",
                dst_path.display()
            ));
        }
    }

    Ok(())
}

/// Restores file times
pub fn restore_times(
    dst_path: &Path,
    atime: Option<&SystemTime>,
    mtime: Option<&SystemTime>,
) -> Result<()> {
    if let Some(modified_time) = mtime {
        let ft_mtime = FileTime::from(*modified_time);
        let ft_atime = atime.map_or(ft_mtime, |atime| FileTime::from(*atime));

        set_file_times(dst_path, ft_atime, ft_mtime).context("Could not set file times")?;
    }

    Ok(())
}

/// Attempts to restore all metadata (times, permissions, ownership, xattrs, flags) for a node.
/// This is a best-effort operation and will log warnings on failure.
#[allow(unused_variables)]
pub(crate) fn try_restore_node_metadata(
    node: &Node,
    dst_path: &Path,
    progress_reporter: &dyn RestoreProgressReporter,
) {
    // Set file times
    if let Err(e) = restore_times(
        dst_path,
        node.metadata.accessed_time.as_ref(),
        node.metadata.modified_time.as_ref(),
    ) {
        progress_reporter.warning(&format!(
            "Could not set file times for {}).",
            dst_path.display()
        ));
    }

    // Unix-specific metadata (mode, uid, gid)
    #[cfg(unix)]
    {
        // Set file permissions (mode)
        if !node.is_symlink()
            && let Some(mode) = node.metadata.mode
        {
            let permissions = Permissions::from_mode(mode);
            if std::fs::set_permissions(dst_path, permissions).is_err() {
                progress_reporter.warning(&format!("Could not set permissions (mode: {mode:o})."));
            }
        }

        if !node.is_symlink() {
            // Set owner (uid) and group (gid)
            let uid = node.metadata.owner_uid;
            let gid = node.metadata.owner_gid;

            // Restoring uid and gid is very likely to fail unless the user is root.
            if uid.is_some() || gid.is_some() {
                let _ = std::os::unix::fs::chown(dst_path, uid, gid);
            }
        }

        // Restore extended attributes
        try_restore_xattrs(node, dst_path, progress_reporter);
    }

    // Restore Linux flags
    #[cfg(target_os = "linux")]
    try_restore_linux_flags(node, dst_path, progress_reporter);

    // Restore Windows attributes
    #[cfg(windows)]
    try_restore_windows_attributes(node, dst_path, progress_reporter);
}

/// Restores extended attributes (xattrs) for a node.
#[cfg(unix)]
fn try_restore_xattrs(
    node: &Node,
    dst_path: &Path,
    progress_reporter: &dyn RestoreProgressReporter,
) {
    if let Some(xattrs) = &node.metadata.extended_attributes {
        for (name, value) in xattrs {
            if let Err(e) = xattr::set(dst_path, name, value) {
                progress_reporter.warning(&format!(
                    "Could not set extended attribute {} for {}: {}",
                    name,
                    dst_path.display(),
                    e
                ));
            }
        }
    }
}

/// Restores Linux-specific file flags for a node.
#[cfg(target_os = "linux")]
fn try_restore_linux_flags(
    node: &Node,
    dst_path: &Path,
    progress_reporter: &dyn RestoreProgressReporter,
) {
    if let Some(flags) = node.metadata.linux_flags {
        if node.is_symlink() {
            return;
        }

        if let Ok(file) = std::fs::File::open(dst_path) {
            // Mask out flags that are not user-modifiable (like FS_EXTENTS_FL)
            // This set includes i, a, d, S, A, etc.
            const FS_FL_USER_MODIFIABLE: u32 = 0x000380FF;
            let flags_to_set = flags & FS_FL_USER_MODIFIABLE;

            let flags_int: libc::c_int = flags_to_set as libc::c_int;
            const FS_IOC_SETFLAGS: libc::c_ulong = 0x40086602;
            unsafe {
                if libc::ioctl(file.as_raw_fd(), FS_IOC_SETFLAGS, &flags_int) != 0 {
                    progress_reporter.warning(&format!(
                        "Could not set Linux flags for {}: {}",
                        dst_path.display(),
                        std::io::Error::last_os_error()
                    ));
                }
            }
        }
    }
}

/// Restores Windows-specific file attributes for a node.
#[cfg(windows)]
fn try_restore_windows_attributes(
    node: &Node,
    dst_path: &Path,
    progress_reporter: &dyn RestoreProgressReporter,
) {
    if let Some(attrs) = node.metadata.windows_attributes {
        use std::os::windows::ffi::OsStrExt;
        let wide_path: Vec<u16> = dst_path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        unsafe {
            if windows_sys::Win32::Storage::FileSystem::SetFileAttributesW(
                wide_path.as_ptr(),
                attrs,
            ) == 0
            {
                progress_reporter.warning(&format!(
                    "Could not set Windows attributes for {}: {}",
                    dst_path.display(),
                    std::io::Error::last_os_error()
                ));
            }
        }
    }
}

/// Restores metadata for a symlink without following it.
#[cfg(unix)]
fn try_restore_symlink_metadata(
    node: &Node,
    dst_path: &Path,
    progress_reporter: &dyn RestoreProgressReporter,
) {
    use std::os::unix::ffi::OsStrExt;

    // Set file times using set_symlink_file_times
    if let Some(mtime) = node.metadata.modified_time.as_ref() {
        let ft_mtime = FileTime::from(*mtime);
        let ft_atime = node.metadata.accessed_time.map_or(ft_mtime, FileTime::from);

        if let Err(e) = filetime::set_symlink_file_times(dst_path, ft_atime, ft_mtime) {
            progress_reporter.warning(&format!(
                "Could not set file times for symlink {}: {}",
                dst_path.display(),
                e
            ));
        }
    }

    // Set owner (uid) and group (gid) using lchown to avoid following the link.
    let uid = node.metadata.owner_uid.unwrap_or(u32::MAX); // -1 in libc
    let gid = node.metadata.owner_gid.unwrap_or(u32::MAX); // -1 in libc

    if uid != u32::MAX || gid != u32::MAX {
        let c_path = std::ffi::CString::new(dst_path.as_os_str().as_bytes()).unwrap();
        unsafe {
            if libc::lchown(c_path.as_ptr(), uid as libc::uid_t, gid as libc::gid_t) != 0 {
                let err = std::io::Error::last_os_error();
                // Only warn if it's not a permission error (which is expected for non-root)
                if err.kind() != std::io::ErrorKind::PermissionDenied {
                    progress_reporter.warning(&format!(
                        "Could not set owner/group for symlink {}: {}",
                        dst_path.display(),
                        err
                    ));
                }
            }
        }
    }

    // Restore extended attributes (xattr crate's set handles symlinks correctly if not following)
    try_restore_xattrs(node, dst_path, progress_reporter);
}

/// Restores metadata for a symlink on Windows.
#[cfg(windows)]
fn try_restore_symlink_metadata(
    node: &Node,
    dst_path: &Path,
    progress_reporter: &dyn RestoreProgressReporter,
) {
    // Set file times for symlink on Windows
    if let Some(mtime) = node.metadata.modified_time.as_ref() {
        let ft_mtime = FileTime::from(*mtime);
        let ft_atime = node.metadata.accessed_time.map_or(ft_mtime, FileTime::from);

        if let Err(e) = filetime::set_symlink_file_times(dst_path, ft_atime, ft_mtime) {
            progress_reporter.warning(&format!(
                "Could not set file times for symlink {}: {}",
                dst_path.display(),
                e
            ));
        }
    }

    // Restore Windows attributes for the symlink itself
    try_restore_windows_attributes(node, dst_path, progress_reporter);
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use {
        crate::ui,
        chrono::{Duration, Local},
        std::time::SystemTime,
    };

    use super::*;

    #[tokio::test]
    async fn test_restore_mtime() -> Result<()> {
        use std::fs::File;

        use tempfile::tempdir;

        let temp_dir = tempdir()?;
        let file_path = temp_dir.path().join("file.txt");

        std::fs::write(&file_path, b"Mapachito").expect("Expected to write to file");
        let mut node = Node::from_path(&file_path).await?;

        // Change mtime to 1 day before now
        let prev_mtime: SystemTime = (Local::now() - Duration::days(1)).into();

        // Manually set the file's current mtime to the past
        let ft_mtime = FileTime::from(prev_mtime);
        let ft_atime = node.metadata.accessed_time.map_or(ft_mtime, FileTime::from);
        set_file_times(&file_path, ft_atime, ft_mtime)
            .with_context(|| format!("Could not set modified time for {}", file_path.display()))?;

        // Create a dummy node with the original metadata to restore from
        let original_metadata = node.metadata.clone();
        let original_mtime = original_metadata.modified_time.unwrap();
        node.metadata = original_metadata;

        // Now restore the metadata from the node
        let reporter = ui::restore::CliRestoreProgressReporter::new(None, None);
        try_restore_node_metadata(&node, &file_path, &reporter);

        // Check if the mtime was restored back to the node's original mtime
        assert_eq!(
            original_mtime,
            file_path.symlink_metadata().unwrap().modified().unwrap()
        );

        Ok(())
    }
}
