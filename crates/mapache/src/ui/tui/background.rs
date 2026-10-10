use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::{
    sync::oneshot::{self, error::TryRecvError},
    task::JoinHandle,
};

pub(crate) struct BackgroundTask<T> {
    signal: Arc<AtomicBool>,
    result: Option<oneshot::Receiver<Result<T, String>>>,
    worker: Option<JoinHandle<()>>,
}

impl<T: Send + 'static> BackgroundTask<T> {
    pub fn spawn<F, Fut>(signal: Arc<AtomicBool>, work: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, String>>,
    {
        let (sender, receiver) = oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| runtime.block_on(work()));
            let _ = sender.send(result);
        });
        Self {
            signal,
            result: Some(receiver),
            worker: Some(worker),
        }
    }

    pub fn spawn_async<Fut>(signal: Arc<AtomicBool>, work: Fut) -> Self
    where
        Fut: Future<Output = Result<T, String>> + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let _ = sender.send(work.await);
        });
        Self {
            signal,
            result: Some(receiver),
            worker: Some(worker),
        }
    }

    pub fn poll(&mut self) -> Option<Result<T, String>> {
        let result = match self.result.as_mut()?.try_recv() {
            Ok(result) => result,
            Err(TryRecvError::Empty) => return None,
            Err(TryRecvError::Closed) => {
                Err("background operation stopped unexpectedly".to_string())
            }
        };
        self.result = None;
        Some(result)
    }

    pub async fn shutdown(&mut self) {
        self.cancel();
        self.wait().await;
    }

    pub fn cancel(&self) {
        self.signal.store(true, Ordering::SeqCst);
    }

    pub async fn wait(&mut self) {
        if let Some(worker) = self.worker.take()
            && let Err(error) = worker.await
        {
            tracing::warn!("Background operation failed: {error}");
        }
    }
}

impl<T> Drop for BackgroundTask<T> {
    fn drop(&mut self) {
        self.signal.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_waits_for_cooperative_worker() {
        let signal = Arc::new(AtomicBool::new(false));
        let worker_signal = signal.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let worker_finished = finished.clone();
        let (started, ready) = oneshot::channel();
        let mut task = BackgroundTask::spawn(signal, move || async move {
            let _ = started.send(());
            while !worker_signal.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            worker_finished.store(true, Ordering::SeqCst);
            Ok(())
        });
        ready.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), task.shutdown())
            .await
            .unwrap();
        assert!(finished.load(Ordering::SeqCst));
        assert_eq!(task.poll(), Some(Ok(())));
    }

    #[tokio::test]
    async fn panicked_worker_is_reported_instead_of_waiting_forever() {
        let mut task = BackgroundTask::<()>::spawn_async(Arc::new(AtomicBool::new(false)), async {
            panic!("test worker panic")
        });
        task.shutdown().await;
        assert!(task.poll().unwrap().is_err());
    }
}
