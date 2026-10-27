use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
    thread,
};

use crossbeam_channel::{Receiver, Sender};

use crate::{
    archiver::{processor, processor::ReusableBuffers, progress::SnapshotProgress},
    common::error::{MapacheError, Result},
    common::traits::BlobSaver,
    fs::tree::{NodeDiff, StreamNode},
    ui::events::EventSender,
};

pub(crate) struct ChunkerJob {
    pub path: PathBuf,
    pub prev_node: Option<StreamNode>,
    pub next_node: Option<StreamNode>,
    pub diff_type: NodeDiff,
    pub blob_saver: Arc<dyn BlobSaver>,
    pub progress: Arc<SnapshotProgress>,
    pub event_sender: EventSender,
    pub shutdown_signal: Arc<AtomicBool>,
    pub skipped_items: Arc<AtomicU64>,
    pub is_stdin: bool,
}

pub(crate) struct ChunkerResult {
    pub path: PathBuf,
    pub result: Result<Option<StreamNode>>,
}

pub(crate) enum ChunkerPoolMsg {
    Single(Box<ChunkerJob>),
}

pub(crate) struct ChunkerPool {
    pub sender: Sender<ChunkerPoolMsg>,
    pub receiver: Receiver<ChunkerResult>,
}

impl ChunkerPool {
    pub(crate) fn new(num_threads: usize) -> Result<Self> {
        if num_threads == 0 {
            return Err(MapacheError::Config(
                "chunker workers must be greater than 0".to_string(),
            ));
        }
        let channel_capacity = num_threads
            .checked_mul(4)
            .ok_or_else(|| MapacheError::Config("chunker worker count is too large".to_string()))?;
        let (sender, job_receiver) = crossbeam_channel::bounded(channel_capacity);
        let (result_sender, receiver) = crossbeam_channel::bounded(channel_capacity);

        let job_receiver = Arc::new(job_receiver);

        for _ in 0..num_threads {
            let rx = job_receiver.clone();
            let tx = result_sender.clone();

            thread::spawn(move || {
                let mut bufs = ReusableBuffers::default();

                let process = |job: Box<ChunkerJob>,
                               bufs: &mut ReusableBuffers|
                 -> (PathBuf, Result<Option<StreamNode>>) {
                    let ChunkerJob {
                        path,
                        prev_node,
                        next_node,
                        diff_type,
                        blob_saver,
                        progress,
                        event_sender,
                        shutdown_signal,
                        skipped_items,
                        is_stdin,
                    } = *job;

                    // IMPORTANT:
                    // Keep the non-stdin as the favourable case for the branch predictor.
                    // Also, stdin would only check this once in the whole snapshot.
                    let result = if !is_stdin {
                        let mut ctx = processor::ItemContext {
                            blob_saver: blob_saver.clone(),
                            progress: progress.as_ref(),
                            event_sender: &event_sender,
                            shutdown_signal: shutdown_signal.as_ref(),
                            skipped_items: skipped_items.as_ref(),
                            bufs: Some(bufs),
                        };
                        processor::process_item_sync(
                            &path, prev_node, next_node, diff_type, &mut ctx,
                        )
                    } else {
                        processor::process_stdin_sync(
                            next_node.as_ref(),
                            processor::StdinReader::new(),
                            blob_saver,
                            progress.as_ref(),
                            &event_sender,
                            shutdown_signal.as_ref(),
                        )
                    };
                    (path, result)
                };

                while let Ok(msg) = rx.recv() {
                    match msg {
                        ChunkerPoolMsg::Single(job) => {
                            let (path, result) = process(job, &mut bufs);
                            if tx.send(ChunkerResult { path, result }).is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }

        Ok(Self { sender, receiver })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_workers() {
        assert!(matches!(
            ChunkerPool::new(0),
            Err(MapacheError::Config(message))
                if message == "chunker workers must be greater than 0"
        ));
    }
}
