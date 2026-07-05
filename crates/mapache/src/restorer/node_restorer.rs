//! The node_restorer module provides lower-level functionality for restoring
//! individual nodes (files, directories, symlinks) and their associated metadata.

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::{fs::OpenOptions, io::Write, path::Path, time::SystemTime};
#[cfg(unix)]
use std::{fs::Permissions, os::unix::fs::PermissionsExt};

use crate::common::error::{MapacheError, Result};
use futures::StreamExt;

use crate::{
    fs::{
        self,
        filetime::FileTime,
        node::{Metadata, Node, NodeType},
    },
    restorer::Restorer,
    ui::events::{Event, EventSender, RestoreEvent, emit_event},
};

/// Restores a node to the specified destination path.
/// This function does not restore file times for directory nodes. This must be
/// done in a reparate pass.
pub(crate) async fn restore_node_to_path(
    restorer: &Restorer,
    event_sender: &EventSender,
    node: &Node,
    dst_path: &Path,
    dry_run: bool,
) -> Result<()> {
    match node.node_type {
        NodeType::File => {
            let blocks = node.blobs.as_ref().ok_or_else(|| {
                MapacheError::Internal("file node must have contents (even if empty)".to_string())
            })?;

            let dst_file = if !dry_run {
                if let Some(parent) = dst_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        MapacheError::Internal(format!(
                            "could not create parent directories for file {}: {e}",
                            dst_path.display()
                        ))
                    })?;
                }

                Some(
                    OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(dst_path)
                        .map_err(|e| {
                            MapacheError::Internal(format!(
                                "could not create file {}: {e}",
                                dst_path.display()
                            ))
                        })?,
                )
            } else {
                None
            };

            let stream = futures::stream::iter(blocks.iter().cloned().enumerate())
                .map(|(index, blob_id)| async move {
                    let res = restorer.repo.load_blob(&blob_id).await.map_err(|e| {
                        MapacheError::Internal(format!(
                            "could not load block #{} ({}) for restoring file {}: {e}",
                            index + 1,
                            blob_id,
                            dst_path.display()
                        ))
                    });
                    (index, blob_id, res)
                })
                .buffered(4); // Prefetch up to 4 blobs in parallel

            futures::pin_mut!(stream);
            while let Some((index, blob_id, chunk_data)) = stream.next().await {
                let chunk_data = chunk_data?;

                // Get a buffer from the pool
                let mut buf = restorer.get_buffer(chunk_data.len());
                buf.clear();
                buf.extend_from_slice(&chunk_data);

                let chunk_size = buf.len() as u64;

                if !dry_run && let Some(mut file) = dst_file.as_ref() {
                    file.write_all(&buf).map_err(|e| {
                        MapacheError::Internal(format!(
                            "could not restore block #{} ({}) to file {}: {e}",
                            index + 1,
                            blob_id,
                            dst_path.display()
                        ))
                    })?;
                }

                emit_event(
                    event_sender,
                    Event::Restore(RestoreEvent::BytesProcessed(chunk_size)),
                );

                // Return the buffer to the pool
                restorer.return_buffer(buf);
            }
        }

        NodeType::Directory => {
            if !dry_run {
                std::fs::create_dir_all(dst_path).map_err(|e| {
                    MapacheError::Internal(format!(
                        "could not create directory {}: {e}",
                        dst_path.display()
                    ))
                })?;

                // We don't restore metadata for directories now, as the filetimes
                // will change if we touch any children nodes. We will restore the
                // directory metadata in a second, dedicated bottom-up pass.
            }
        }

        NodeType::Symlink => {
            let symlink_info = node.symlink_info.as_ref();

            // Show a warning if the symlink metadata is missing and return.
            let Some(symlink_info) = symlink_info else {
                emit_event(
                    event_sender,
                    Event::Restore(RestoreEvent::Warning(format!(
                        "Symlink {} does not have a target path",
                        dst_path.display()
                    ))),
                );
                return Ok(());
            };

            if !dry_run {
                // Create all parent directories before the symlink
                if let Some(parent) = dst_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        MapacheError::Internal(format!(
                            "could not create parent directories for symlink {dst_path:?}: {e}"
                        ))
                    })?;
                }

                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(&symlink_info.target_path, dst_path).map_err(
                        |e| {
                            MapacheError::Internal(format!(
                                "could not create symlink {dst_path:?}: {e}"
                            ))
                        },
                    )?;
                }
                #[cfg(windows)]
                {
                    match symlink_info.target_type {
                        // Directory symlink
                        Some(NodeType::Directory) => {
                            std::os::windows::fs::symlink_dir(&symlink_info.target_path, dst_path)
                                .map_err(|e| {
                                    MapacheError::Internal(format!(
                                        "could not create directory symlink {dst_path:?}: {e}"
                                    ))
                                })?;
                        }

                        // Everything else (not a directory)
                        Some(_) => {
                            std::os::windows::fs::symlink_file(&symlink_info.target_path, dst_path)
                                .map_err(|e| {
                                    MapacheError::Internal(format!(
                                        "could not create file symlink {dst_path:?}: {e}"
                                    ))
                                })?;
                        }
                        // No type info. Show warning.
                        None => {
                            emit_event(
                                event_sender,
                                Event::Restore(RestoreEvent::Warning(format!(
                                    "Symlink {} has no type info",
                                    dst_path.display()
                                ))),
                            );
                        }
                    }
                }
            }

            // Restore symlink metadata after creation
            if !dry_run {
                try_restore_symlink_metadata(&node.metadata, dst_path, event_sender);
            }
        }

        NodeType::BlockDevice => {
            #[cfg(unix)]
            emit_event(
                event_sender,
                Event::Restore(RestoreEvent::Warning(format!(
                    "Restoration of block device {} not supported yet.",
                    dst_path.display()
                ))),
            );
            #[cfg(not(unix))]
            emit_event(
                event_sender,
                Event::Restore(RestoreEvent::Warning(format!(
                    "Block device restoration not supported on this operating system: {}",
                    dst_path.display()
                ))),
            );
        }

        NodeType::CharDevice => {
            #[cfg(unix)]
            emit_event(
                event_sender,
                Event::Restore(RestoreEvent::Warning(format!(
                    "Restoration of character device {} not supported yet.",
                    dst_path.display()
                ))),
            );
            #[cfg(not(unix))]
            emit_event(
                event_sender,
                Event::Restore(RestoreEvent::Warning(format!(
                    "Character device restoration not supported on this operating system: {}",
                    dst_path.display()
                ))),
            );
        }

        NodeType::Fifo => {
            #[cfg(unix)]
            emit_event(
                event_sender,
                Event::Restore(RestoreEvent::Warning(format!(
                    "Restoration of FIFO (named pipe) {} not supported yet.",
                    dst_path.display()
                ))),
            );
            #[cfg(not(unix))]
            emit_event(
                event_sender,
                Event::Restore(RestoreEvent::Warning(format!(
                    "FIFO restoration not supported on this operating system: {}",
                    dst_path.display()
                ))),
            );
        }

        NodeType::Socket => {
            #[cfg(unix)]
            emit_event(
                event_sender,
                Event::Restore(RestoreEvent::Warning(format!(
                    "Restoration of socket {} not supported yet.",
                    dst_path.display()
                ))),
            );
            #[cfg(not(unix))]
            emit_event(
                event_sender,
                Event::Restore(RestoreEvent::Warning(format!(
                    "Socket restoration not supported on this operating system: {}",
                    dst_path.display()
                ))),
            );
        }
    }

    Ok(())
}

/// Restores file times
pub fn restore_times(
    dst_path: &Path,
    atime: Option<&SystemTime>,
    mtime: Option<&SystemTime>,
    is_symlink: bool,
) -> Result<()> {
    if let Some(modified_time) = mtime {
        let ft_mtime = FileTime::from(*modified_time);
        let ft_atime = atime.map_or(ft_mtime, |atime| FileTime::from(*atime));

        if is_symlink {
            fs::filetime::set_symlink_file_times(dst_path, ft_atime, ft_mtime).map_err(|e| {
                MapacheError::Internal(format!("could not set symlink file times: {e}"))
            })?;
        } else {
            fs::filetime::set_file_times(dst_path, ft_atime, ft_mtime)
                .map_err(|e| MapacheError::Internal(format!("could not set file times: {e}")))?;
        }
    }

    Ok(())
}

/// Attempts to restore all metadata (times, permissions, ownership, xattrs, flags) for a node.
/// This is a best-effort operation and will log warnings on failure.
///
/// The order is important:
/// 1. chown — must come first because chown clears setuid/setgid and security.capability
/// 2. xattrs — applied while the inode is still owner-writable (before a potentially read-only final mode)
/// 3. chmod — after xattrs, final mode
/// 4. mtime — last, because chown/chmod/setxattr bump ctime, not mtime
pub(crate) fn try_restore_node_metadata(
    metadata: &Metadata,
    is_symlink: bool,
    dst_path: &Path,
    event_sender: &EventSender,
) {
    if is_symlink {
        try_restore_symlink_metadata(metadata, dst_path, event_sender);
        return;
    }

    // Unix-specific metadata (ownership, permissions, xattrs)
    #[cfg(unix)]
    {
        // 1. Set owner (uid) and group (gid) — must come first
        // Restoring uid and gid is very likely to fail unless the user is root.
        if metadata.owner_uid.is_some() || metadata.owner_gid.is_some() {
            let _ = std::os::unix::fs::chown(dst_path, metadata.owner_uid, metadata.owner_gid);
        }

        // 2. Restore extended attributes — before chmod because xattr access may be restricted by mode
        try_restore_xattrs(metadata, dst_path, event_sender);
    }

    // Restore Linux flags (before chmod, as flags can interact with permission bits)
    #[cfg(target_os = "linux")]
    try_restore_linux_flags(metadata, false, dst_path, event_sender);

    // Restore Windows attributes
    #[cfg(windows)]
    try_restore_windows_attributes(metadata, dst_path, event_sender);

    // 3. Set file permissions (mode)
    #[cfg(unix)]
    if let Some(mode) = metadata.mode {
        let permissions = Permissions::from_mode(mode);
        if std::fs::set_permissions(dst_path, permissions).is_err() {
            emit_event(
                event_sender,
                Event::Restore(RestoreEvent::Warning(format!(
                    "Could not set permissions (mode: {mode:o})."
                ))),
            );
        }
    }

    // 4. Set file times — last, because chown/chmod/setxattr bump ctime, not mtime
    if let Err(e) = restore_times(
        dst_path,
        metadata.accessed_time.as_ref(),
        metadata.modified_time.as_ref(),
        false,
    ) {
        emit_event(
            event_sender,
            Event::Restore(RestoreEvent::Warning(format!(
                "Could not set file times for {}: {}.",
                dst_path.display(),
                e
            ))),
        );
    }
}

/// Restores extended attributes (xattrs) for a node.
#[cfg(unix)]
fn try_restore_xattrs(metadata: &Metadata, dst_path: &Path, event_sender: &EventSender) {
    if let Some(xattrs) = &metadata.extended_attributes {
        for (name, value) in xattrs {
            if let Err(e) = xattr::set(dst_path, name, value) {
                emit_event(
                    event_sender,
                    Event::Restore(RestoreEvent::Warning(format!(
                        "Could not set extended attribute {} for {}: {}",
                        name,
                        dst_path.display(),
                        e
                    ))),
                );
            }
        }
    }
}

/// Restores Linux-specific file flags for a node.
#[cfg(target_os = "linux")]
fn try_restore_linux_flags(
    metadata: &Metadata,
    is_symlink: bool,
    dst_path: &Path,
    event_sender: &EventSender,
) {
    if let Some(flags) = metadata.linux_flags {
        if is_symlink {
            return;
        }

        if let Ok(file) = std::fs::File::open(dst_path) {
            // Mask out flags that are not user-modifiable (like FS_EXTENTS_FL)
            // This set includes i, a, d, S, A, etc.
            const FS_FL_USER_MODIFIABLE: u32 = 0x000380FF;
            let flags_to_set = flags & FS_FL_USER_MODIFIABLE;

            let flags_int: libc::c_int = flags_to_set as libc::c_int;
            const FS_IOC_SETFLAGS: libc::Ioctl = 0x40086602u32 as libc::Ioctl;
            unsafe {
                // SAFETY: We are calling ioctl on a valid file descriptor with a correctly
                // sized flags integer. FS_IOC_SETFLAGS is a standard Linux ioctl for
                // setting file attributes.
                if libc::ioctl(file.as_raw_fd(), FS_IOC_SETFLAGS, &flags_int) != 0 {
                    emit_event(
                        event_sender,
                        Event::Restore(RestoreEvent::Warning(format!(
                            "Could not set Linux flags for {}: {}",
                            dst_path.display(),
                            std::io::Error::last_os_error()
                        ))),
                    );
                }
            }
        }
    }
}

/// Restores Windows-specific file attributes for a node.
#[cfg(windows)]
fn try_restore_windows_attributes(
    metadata: &Metadata,
    dst_path: &Path,
    event_sender: &EventSender,
) {
    if let Some(attrs) = metadata.windows_attributes {
        use std::os::windows::ffi::OsStrExt;
        let wide_path: Vec<u16> = dst_path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        unsafe {
            // SAFETY: wide_path is a null-terminated UTF-16 string, which is required
            // by the Windows API SetFileAttributesW.
            if windows_sys::Win32::Storage::FileSystem::SetFileAttributesW(
                wide_path.as_ptr(),
                attrs,
            ) == 0
            {
                emit_event(
                    event_sender,
                    Event::Restore(RestoreEvent::Warning(format!(
                        "Could not set Windows attributes for {}: {}",
                        dst_path.display(),
                        std::io::Error::last_os_error()
                    ))),
                );
            }
        }
    }
}

/// Restores metadata for a symlink without following it.
///
/// Order: lchown → xattrs → mtime (lchown must come first because it clears
/// security.capability; xattrs before mtime because mtime should be last).
#[cfg(unix)]
fn try_restore_symlink_metadata(metadata: &Metadata, dst_path: &Path, event_sender: &EventSender) {
    use std::os::unix::ffi::OsStrExt;

    // 1. Set owner (uid) and group (gid) using lchown to avoid following the link.
    let uid = metadata.owner_uid.unwrap_or(u32::MAX); // -1 in libc
    let gid = metadata.owner_gid.unwrap_or(u32::MAX); // -1 in libc

    if uid != u32::MAX || gid != u32::MAX {
        match std::ffi::CString::new(dst_path.as_os_str().as_bytes()) {
            Ok(c_path) => unsafe {
                // SAFETY: c_path is a valid null-terminated C string. lchown is a
                // standard libc function that operates on symlinks without following them.
                if libc::lchown(c_path.as_ptr(), uid as libc::uid_t, gid as libc::gid_t) != 0 {
                    let err = std::io::Error::last_os_error();
                    // Only warn if it's not a permission error (which is expected for non-root)
                    if err.kind() != std::io::ErrorKind::PermissionDenied {
                        emit_event(
                            event_sender,
                            Event::Restore(RestoreEvent::Warning(format!(
                                "Could not set owner/group for symlink {}: {}",
                                dst_path.display(),
                                err
                            ))),
                        );
                    }
                }
            },
            Err(_) => {
                emit_event(
                    event_sender,
                    Event::Restore(RestoreEvent::Warning(format!(
                        "Could not set owner/group for symlink {}: path contains null byte",
                        dst_path.display()
                    ))),
                );
            }
        }
    }

    // 2. Restore extended attributes (xattr crate's set handles symlinks correctly if not following)
    try_restore_xattrs(metadata, dst_path, event_sender);

    // 3. Set file times using set_symlink_file_times — last
    if let Err(e) = restore_times(
        dst_path,
        metadata.accessed_time.as_ref(),
        metadata.modified_time.as_ref(),
        true,
    ) {
        emit_event(
            event_sender,
            Event::Restore(RestoreEvent::Warning(format!(
                "Could not set file times for symlink {}: {}",
                dst_path.display(),
                e
            ))),
        );
    }
}

/// Restores metadata for a symlink on Windows.
#[cfg(windows)]
fn try_restore_symlink_metadata(metadata: &Metadata, dst_path: &Path, event_sender: &EventSender) {
    // Restore Windows attributes for the symlink itself — before mtime
    try_restore_windows_attributes(metadata, dst_path, event_sender);

    // Set file times for symlink — last
    if let Err(e) = restore_times(
        dst_path,
        metadata.accessed_time.as_ref(),
        metadata.modified_time.as_ref(),
        true,
    ) {
        emit_event(
            event_sender,
            Event::Restore(RestoreEvent::Warning(format!(
                "Could not set file times for symlink {}: {}",
                dst_path.display(),
                e
            ))),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use chrono::{Duration, Local};
    use tempfile::tempdir;

    use crate::ui::events::noop_sender;

    use super::*;

    #[tokio::test]
    async fn test_restore_mtime() -> Result<()> {
        let temp_dir = tempdir()?;
        let file_path = temp_dir.path().join("file.txt");

        std::fs::write(&file_path, b"Mapachito").expect("Expected to write to file");
        let mut node = Node::from_path(&file_path, false).await?;

        // Change mtime to 1 day before now
        let prev_mtime: SystemTime = (Local::now() - Duration::days(1)).into();

        // Manually set the file's current mtime to the past
        let ft_mtime = FileTime::from(prev_mtime);
        let ft_atime = node.metadata.accessed_time.map_or(ft_mtime, FileTime::from);
        fs::filetime::set_file_times(&file_path, ft_atime, ft_mtime).map_err(|e| {
            MapacheError::Internal(format!(
                "Could not set modified time for {}: {e}",
                file_path.display()
            ))
        })?;

        // Create a dummy node with the original metadata to restore from
        let original_metadata = node.metadata.clone();
        let original_mtime = original_metadata
            .modified_time
            .expect("file node must have modified_time");
        node.metadata = original_metadata;

        // Now restore the metadata from the node
        let sender = noop_sender();
        try_restore_node_metadata(&node.metadata, false, &file_path, &sender);

        // Check if the mtime was restored back to the node's original mtime
        assert_eq!(
            original_mtime,
            file_path
                .symlink_metadata()
                .expect("file exists after write")
                .modified()
                .expect("file must have modified time")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_atime() -> Result<()> {
        let temp_dir = tempdir()?;
        let file_path = temp_dir.path().join("file.txt");

        std::fs::write(&file_path, b"Mapachito").expect("Expected to write to file");

        // Set specific atime and mtime (1 day and 2 days ago respectively)
        let original_atime: SystemTime = (Local::now() - Duration::days(1)).into();
        let original_mtime: SystemTime = (Local::now() - Duration::days(2)).into();

        let ft_atime = FileTime::from(original_atime);
        let ft_mtime = FileTime::from(original_mtime);
        fs::filetime::set_file_times(&file_path, ft_atime, ft_mtime).map_err(|e| {
            MapacheError::Internal(format!(
                "Could not set file times for {}: {e}",
                file_path.display()
            ))
        })?;

        // Capture node metadata WITH atime
        let node = Node::from_path(&file_path, true).await?;
        let captured_atime = node
            .metadata
            .accessed_time
            .expect("captured with atime=true, so accessed_time must be Some");
        let captured_mtime = node
            .metadata
            .modified_time
            .expect("file node must have modified_time");

        // Now change the file times to something else (now)
        let now: SystemTime = Local::now().into();
        let ft_now = FileTime::from(now);
        fs::filetime::set_file_times(&file_path, ft_now, ft_now)?;

        // Restore metadata from the captured node
        let sender = noop_sender();
        try_restore_node_metadata(&node.metadata, false, &file_path, &sender);

        // Verify both atime and mtime were restored
        let restored_meta = file_path.symlink_metadata()?;
        let restored_atime = restored_meta.accessed()?;
        let restored_mtime = restored_meta.modified()?;

        let atime_diff = captured_atime
            .duration_since(restored_atime)
            .unwrap_or_else(|_| restored_atime.duration_since(captured_atime).unwrap());
        assert!(
            atime_diff.as_secs() <= 1,
            "restored atime differs by {:?}",
            atime_diff
        );

        let mtime_diff = captured_mtime
            .duration_since(restored_mtime)
            .unwrap_or_else(|_| restored_mtime.duration_since(captured_mtime).unwrap());
        assert!(
            mtime_diff.as_secs() <= 1,
            "restored mtime differs by {:?}",
            mtime_diff
        );

        Ok(())
    }
}
