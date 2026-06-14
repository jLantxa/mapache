use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use anyhow::Result;

use crate::repository::lock::LockHandle;

pub struct CleanupHandler {
    pub interrupted: Arc<AtomicBool>,
    locks: Arc<Mutex<Vec<LockHandle>>>,
    abort_handle: tokio::task::AbortHandle,
}

impl CleanupHandler {
    pub fn new() -> Result<Self> {
        Self::new_with_callback(|| {})
    }

    pub fn new_with_callback<F>(callback: F) -> Result<Self>
    where
        F: Fn() + Send + 'static,
    {
        let interrupted = Arc::new(AtomicBool::new(false));
        let locks = Arc::new(Mutex::new(Vec::<LockHandle>::new()));

        let interrupted_clone = interrupted.clone();
        let locks_clone = locks.clone();

        let join_handle = tokio::spawn(async move {
            wait_for_signal().await;
            interrupted_clone.store(true, Ordering::SeqCst);
            callback();
            if let Ok(locks) = locks_clone.lock() {
                for lock in locks.iter() {
                    lock.trigger_unlock();
                }
            }
        });

        Ok(Self {
            interrupted,
            locks,
            abort_handle: join_handle.abort_handle(),
        })
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted.load(Ordering::SeqCst)
    }

    pub fn add_lock(&self, lock: LockHandle) {
        if let Ok(mut locks) = self.locks.lock() {
            locks.push(lock);
        }
    }
}

impl Drop for CleanupHandler {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let sigint = signal(SignalKind::interrupt());
    let sigterm = signal(SignalKind::terminate());
    if let (Ok(mut sigint), Ok(mut sigterm)) = (sigint, sigterm) {
        tokio::select! {
            _ = sigint.recv() => {},
            _ = sigterm.recv() => {},
        }
    } else {
        tracing::warn!("Failed to register signal handlers; cleanup will not be interruptible");
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
