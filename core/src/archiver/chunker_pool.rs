use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    thread,
};

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};

use crate::{
    archiver::{processor, processor::ReusableBuffers, progress::SnapshotProgress},
    fs::tree::{NodeDiff, StreamNode},
    mapache::traits::BlobSaver,
    ui::SnapshotProgressReporter,
};

pub(crate) const BATCH_SIZE: usize = 16;

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

pub(crate) enum ChunkerPoolMsg {
    Single(Box<ChunkerJob>),
    Batch(Vec<ChunkerJob>),
}

pub(crate) struct ChunkerPool {
    pub sender: Sender<ChunkerPoolMsg>,
    pub receiver: Receiver<ChunkerResult>,
}

impl ChunkerPool {
    pub(crate) fn new(num_threads: usize) -> Self {
        let (sender, job_receiver) = crossbeam_channel::bounded::<ChunkerPoolMsg>(num_threads * 4);
        let (result_sender, receiver) =
            crossbeam_channel::bounded::<ChunkerResult>(num_threads * 4);

        let job_receiver = Arc::new(job_receiver);

        for _ in 0..num_threads {
            let rx = job_receiver.clone();
            let tx = result_sender.clone();

            thread::spawn(move || {
                let mut bufs = ReusableBuffers::default();

                let process =
                    |job: &ChunkerJob, bufs: &mut ReusableBuffers| -> Result<Option<StreamNode>> {
                        processor::process_item_sync(
                            &job.path,
                            job.prev_node.as_ref(),
                            job.next_node.as_ref(),
                            job.diff_type,
                            job.blob_saver.clone(),
                            job.progress.as_ref(),
                            job.progress_reporter.as_ref(),
                            job.shutdown_signal.as_ref(),
                            Some(bufs),
                        )
                    };

                while let Ok(msg) = rx.recv() {
                    match msg {
                        ChunkerPoolMsg::Single(job) => {
                            let result = process(&job, &mut bufs);
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
                        ChunkerPoolMsg::Batch(jobs) => {
                            for job in jobs {
                                let result = process(&job, &mut bufs);
                                if tx
                                    .send(ChunkerResult {
                                        path: job.path,
                                        result,
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                }
            });
        }

        Self { sender, receiver }
    }
}
