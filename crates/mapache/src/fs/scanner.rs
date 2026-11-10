use std::{collections::VecDeque, path::PathBuf, sync::Arc};

use crate::{
    common::error::Result,
    fs::{
        filesystem::{ScanMetadata, fill_device_id, scan_metadata},
        filter::{PathFilter, WalkOptions},
        node::NodeType,
        tree::crosses_filesystem_boundary,
    },
    ui::events::{BackupEvent, Event, EventSender, emit_event},
};

#[derive(Debug, Clone, Copy, Default)]
pub struct ScanStats {
    pub total_items: u64,
    pub total_bytes: u64,
}

/// Performs a synchronous directory walk to count items and estimate total size.
/// This is designed to run inside a blocking task.
pub fn scan_directories<F>(
    paths: &[PathBuf],
    filter: Arc<PathFilter>,
    walk_options: &WalkOptions,
    one_file_system: bool,
    event_sender: &EventSender,
    mut is_cancelled: F,
) -> ScanStats
where
    F: FnMut() -> bool,
{
    use rayon::prelude::*;

    let mut scan_items = 0u64;
    let mut scan_bytes = 0u64;
    // Each entry carries the device of its parent and, when the parent has
    // already statted it, the cached metadata.
    let mut stack: VecDeque<(PathBuf, Option<u64>, Option<ScanMetadata>)> = paths
        .iter()
        .cloned()
        .map(|path| (path, None, None))
        .collect();

    while let Some((current, parent_device, cached)) = stack.pop_front() {
        if is_cancelled() {
            break;
        }

        if !filter.allow(&current) {
            continue;
        }

        let mut node = match cached {
            Some(node) => node,
            None => match scan_metadata(&current) {
                Ok(node) => node,
                Err(e) => {
                    emit_event(
                        event_sender,
                        Event::Backup(BackupEvent::Warning(format!(
                            "error scanning {}: {}",
                            current.display(),
                            e
                        ))),
                    );
                    continue;
                }
            },
        };

        if one_file_system {
            if node.node_type == NodeType::Directory {
                fill_device_id(&mut node.dev, &current);
            }
            if crosses_filesystem_boundary(parent_device, node.dev) {
                continue;
            }
        }

        let size = if node.node_type == NodeType::File {
            node.size
        } else {
            0
        };

        // Keep the progress estimate in sync with the archiver's
        // `--exclude-larger-than`: oversized files are not snapshotted,
        // so they must not inflate the expected item/byte counts.
        if node.node_type == NodeType::File
            && walk_options
                .exclude_larger_than
                .is_some_and(|max| size > max)
        {
            continue;
        }

        scan_items += 1;
        scan_bytes += size;

        emit_event(
            event_sender,
            Event::Backup(BackupEvent::ScanProgress {
                items: 1,
                bytes: size,
            }),
        );

        if node.node_type != NodeType::Directory {
            continue;
        }

        // Enumerate the shadow copy when a VSS session is active;
        // child names are identical, and the walk itself stays in
        // the original path space.
        let read_dir_path = crate::fs::vss::translate(&current);
        let entries = match std::fs::read_dir(&*read_dir_path).and_then(|entries| {
            entries
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<std::io::Result<Vec<_>>>()
        }) {
            Ok(entries) => entries,
            Err(e) => {
                let msg = format!("error reading directory {}: {}", current.display(), e);
                emit_event(event_sender, Event::Backup(BackupEvent::Warning(msg)));
                continue;
            }
        };

        // Mirror `--exclude-if-present`: when a marker file
        // is present, only the marker files are kept.
        let entries: Vec<_> = if walk_options
            .exclude_if_present
            .iter()
            .any(|marker| entries.iter().any(|name| name == marker))
        {
            entries
                .into_iter()
                .filter(|name| {
                    walk_options
                        .exclude_if_present
                        .iter()
                        .any(|marker| name == marker)
                })
                .collect()
        } else {
            entries
        };

        let children: Vec<PathBuf> = entries
            .into_iter()
            .map(|name| current.join(name))
            .filter(|child| filter.allow(child))
            .collect();

        // Stat this directory's children in parallel. This mirrors the
        // archiver's walk, so the scan keeps up with (and usually finishes
        // ahead of) the archival pipeline.
        let statted: Vec<(PathBuf, Result<ScanMetadata>)> = children
            .into_par_iter()
            .map(|child_path| {
                let mut child = scan_metadata(&child_path);
                if one_file_system
                    && let Ok(node) = &mut child
                    && node.node_type == NodeType::Directory
                {
                    fill_device_id(&mut node.dev, &child_path);
                }
                (child_path, child)
            })
            .collect();

        let child_parent_device = node.dev;
        for (child_path, child) in statted {
            match child {
                Ok(node) => {
                    if one_file_system && crosses_filesystem_boundary(child_parent_device, node.dev)
                    {
                        continue;
                    }
                    stack.push_back((child_path, child_parent_device, Some(node)));
                }
                Err(e) => {
                    emit_event(
                        event_sender,
                        Event::Backup(BackupEvent::Warning(format!(
                            "error scanning {}: {}",
                            child_path.display(),
                            e
                        ))),
                    );
                }
            }
        }
    }

    ScanStats {
        total_items: scan_items,
        total_bytes: scan_bytes,
    }
}

/// Spawns a background scanner task that walks the filesystem, emits progress,
/// and finishes with ScanFinished.
pub fn spawn_background_scanner<F>(
    paths: Vec<PathBuf>,
    exclude_paths: Vec<PathBuf>,
    walk_options: WalkOptions,
    one_file_system: bool,
    event_sender: EventSender,
    is_cancelled: F,
) -> tokio::task::JoinHandle<std::result::Result<ScanStats, String>>
where
    F: Fn() -> bool + Send + Sync + 'static,
{
    tokio::spawn(async move {
        emit_event(&event_sender, Event::Backup(BackupEvent::ScanStarted));

        let filter = Arc::new(PathFilter::new(None, Some(exclude_paths)));
        let sender = event_sender.clone();
        let res = tokio::task::spawn_blocking(move || {
            scan_directories(
                &paths,
                filter,
                &walk_options,
                one_file_system,
                &sender,
                is_cancelled,
            )
        })
        .await;

        match res {
            Ok(stats) => {
                emit_event(
                    &event_sender,
                    Event::Backup(BackupEvent::ScanFinished {
                        total_items: stats.total_items,
                        total_bytes: stats.total_bytes,
                    }),
                );
                Ok(stats)
            }
            Err(e) => {
                let err_msg = format!("background scanner panicked: {e}");
                emit_event(
                    &event_sender,
                    Event::Backup(BackupEvent::Error(err_msg.clone())),
                );
                Err(err_msg)
            }
        }
    })
}
