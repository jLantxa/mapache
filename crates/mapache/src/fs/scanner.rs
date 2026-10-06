use std::{collections::VecDeque, path::PathBuf, sync::Arc};

use crate::{
    fs::{
        filesystem::scan_metadata, filter::PathFilter, node::NodeType,
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
    one_file_system: bool,
    event_sender: &EventSender,
    mut is_cancelled: F,
) -> ScanStats
where
    F: FnMut() -> bool,
{
    let mut scan_items = 0u64;
    let mut scan_bytes = 0u64;
    let mut stack: VecDeque<(PathBuf, Option<u64>)> =
        paths.iter().cloned().map(|path| (path, None)).collect();

    while let Some((current, parent_device)) = stack.pop_front() {
        if is_cancelled() {
            break;
        }

        if !filter.allow(&current) {
            continue;
        }

        match scan_metadata(&current) {
            Ok(node) => {
                if one_file_system && crosses_filesystem_boundary(parent_device, node.dev) {
                    continue;
                }

                let size = if node.node_type == NodeType::File {
                    node.size
                } else {
                    0
                };

                scan_items += 1;
                scan_bytes += size;

                emit_event(
                    event_sender,
                    Event::Backup(BackupEvent::ScanProgress {
                        items: 1,
                        bytes: size,
                    }),
                );

                if node.node_type == NodeType::Directory {
                    match std::fs::read_dir(&current).and_then(|entries| {
                        entries
                            .map(|entry| entry.map(|entry| entry.file_name()))
                            .collect::<std::io::Result<Vec<_>>>()
                    }) {
                        Ok(entries) => {
                            for name in entries {
                                stack.push_back((current.join(name), node.dev));
                            }
                        }
                        Err(e) => {
                            let msg =
                                format!("error reading directory {}: {}", current.display(), e);
                            emit_event(event_sender, Event::Backup(BackupEvent::Warning(msg)));
                        }
                    }
                }
            }
            Err(e) => {
                emit_event(
                    event_sender,
                    Event::Backup(BackupEvent::Warning(format!(
                        "error scanning {}: {}",
                        current.display(),
                        e
                    ))),
                );
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
            scan_directories(&paths, filter, one_file_system, &sender, is_cancelled)
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
