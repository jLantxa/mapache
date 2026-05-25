use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};

use crate::{
    archiver::{processor, progress::SnapshotProgress},
    fs::tree::{NodeDiff, StreamNode},
    mapache::traits::BlobSaver,
    ui::SnapshotProgressReporter,
};

pub(crate) struct ChunkerJob {
    pub path: PathBuf,
    pub prev_node: Option<StreamNode>,
    pub next_node: Option<StreamNode>,
    pub diff_type: NodeDiff,
    pub blob_saver: Arc<dyn BlobSaver>,
    pub progress: Arc<SnapshotProgress>,
    pub progress_reporter: Arc<dyn SnapshotProgressReporter>,
    pub shutdown_signal: Arc<AtomicBool>,
}

pub(crate) struct ChunkerResult {
    pub path: PathBuf,
    pub result: Result<Option<StreamNode>>,
}

pub(crate) struct ChunkerPool {
    pub sender: Sender<ChunkerJob>,
    pub receiver: Receiver<ChunkerResult>,
}

impl ChunkerPool {
    pub(crate) fn new(num_threads: usize) -> Self {
        let (sender, job_receiver) = crossbeam_channel::bounded::<ChunkerJob>(num_threads * 4);
        let (result_sender, receiver) =
            crossbeam_channel::bounded::<ChunkerResult>(num_threads * 4);

        let job_receiver = Arc::new(job_receiver);

        for _ in 0..num_threads {
            let rx = job_receiver.clone();
            let tx = result_sender.clone();

            thread::spawn(move || {
                while let Ok(job) = rx.recv() {
                    if job.shutdown_signal.load(Ordering::Relaxed) {
                        break;
                    }

                    let result = processor::process_item_sync(
                        &job.path,
                        job.prev_node.as_ref(),
                        job.next_node.as_ref(),
                        job.diff_type,
                        job.blob_saver,
                        job.progress.as_ref(),
                        job.progress_reporter.as_ref(),
                        job.shutdown_signal.as_ref(),
                    );

                    if tx
                        .send(ChunkerResult {
                            path: job.path,
                            result,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }

        Self { sender, receiver }
    }
}
