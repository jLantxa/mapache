use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
};

use crate::common::error::{MapacheError, Result};
use futures::StreamExt;
use parking_lot::Mutex;

use crate::{
    common::ID,
    fs::{self as repo_fs, node::Metadata, tree::SerializedNodeStream},
    ui::events::{Event, RestoreEvent, emit_event},
    utils,
};

use super::Restorer;

impl Restorer {
    pub(crate) async fn restore_metadata(
        &self,
        tree_id: ID,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
    ) -> Result<()> {
        if self.opts.dry_run {
            return Ok(());
        }

        let node_stream = SerializedNodeStream::new(
            self.repo.clone(),
            Some(tree_id),
            PathBuf::new(),
            include,
            exclude,
        )
        .await?;

        let dirs: Arc<Mutex<Vec<(PathBuf, Metadata)>>> = Arc::new(Mutex::new(Vec::new()));

        let d = crate::common::defaults::runtime();
        node_stream
            .for_each_concurrent(d.restore_blob_concurrency, |node_res| {
                let event_sender = self.event_sender.clone();
                let target_path = self.target_path.clone();
                let opts_strip_prefix = self.opts.strip_prefix.clone();
                let dirs = dirs.clone();

                async move {
                    let (mut path, stream_node_res) = match node_res {
                        Ok(res) => res,
                        Err(e) => {
                            emit_event(
                                &event_sender,
                                Event::Restore(RestoreEvent::Warning(format!(
                                    "failed to read node from stream: {e}"
                                ))),
                            );
                            return;
                        }
                    };

                    let stream_node = match stream_node_res {
                        Ok(node) => node,
                        Err(e) => {
                            emit_event(
                                &event_sender,
                                Event::Restore(RestoreEvent::Warning(format!(
                                    "failed to deserialize node: {e}"
                                ))),
                            );
                            return;
                        }
                    };
                    let node = stream_node.node;

                    if let Some(prefix) = &opts_strip_prefix {
                        path = match path.strip_prefix(prefix) {
                            Ok(stripped_path) => {
                                if stripped_path.as_os_str().is_empty() {
                                    return;
                                }
                                stripped_path.to_path_buf()
                            }
                            Err(_) => return,
                        };
                    }

                    let restore_path = match utils::secure_join(&target_path, &path) {
                        Ok(p) => p,
                        Err(_) => return,
                    };

                    if node.is_dir() {
                        dirs.lock().push((restore_path, node.metadata));
                    } else {
                        if repo_fs::path_exists(&restore_path).await {
                            super::node_restorer::try_restore_node_metadata(
                                &node.metadata,
                                node.is_symlink(),
                                &restore_path,
                                &event_sender,
                            );
                        }
                        emit_event(
                            &event_sender,
                            Event::Restore(RestoreEvent::ItemProcessed(restore_path)),
                        );
                    }
                }
            })
            .await;

        let mut dirs = match Arc::try_unwrap(dirs) {
            Ok(mutex) => mutex.into_inner(),
            Err(arc) => {
                let guard = arc.lock();
                guard.clone()
            }
        };
        dirs.sort_unstable_by_key(|(p, _)| std::cmp::Reverse(p.as_os_str().len()));
        for (p, meta) in dirs {
            if self.shutdown_signal.load(Ordering::Acquire) {
                return Err(MapacheError::Interrupted);
            }
            if repo_fs::path_exists(&p).await {
                super::node_restorer::try_restore_node_metadata(
                    &meta,
                    false,
                    &p,
                    &self.event_sender,
                );
            }
        }

        Ok(())
    }
}
