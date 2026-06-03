use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use anyhow::Result;
use signal_hook_registry::{SigId, register, unregister};

use crate::repository::lock::LockHandle;

pub struct CleanupHandler {
    sigint_handler: SigId,
    sigterm_handler: SigId,
    pub interrupted: Arc<AtomicBool>,
    locks: Arc<Mutex<Vec<LockHandle>>>,
}

impl CleanupHandler {
    /// Register callbacks for the SIGINT and SIGTERM signals.
    pub fn new() -> Result<Self> {
        let interrupted = Arc::new(AtomicBool::new(false));
        let locks = Arc::new(Mutex::new(Vec::<LockHandle>::new()));

        let sigint_handler = unsafe {
            // SAFETY: the closure only sets an atomic flag, which is async-signal-safe.
            register(libc::SIGINT, {
                let flag = interrupted.clone();
                move || flag.store(true, Ordering::SeqCst)
            })?
        };
        let sigterm_handler = unsafe {
            // SAFETY: the closure only sets an atomic flag, which is async-signal-safe.
            register(libc::SIGTERM, {
                let flag = interrupted.clone();
                move || flag.store(true, Ordering::SeqCst)
            })?
        };

        spawn_cleanup_worker(interrupted.clone(), locks.clone());

        Ok(CleanupHandler {
            sigint_handler,
            sigterm_handler,
            interrupted,
            locks,
        })
    }

    /// Register callbacks for the SIGINT and SIGTERM signals with an immediate UI callback.
    ///
    /// The callback runs on a dedicated daemon thread after the signal fires, not inside the
    /// signal handler itself, so it may use any Rust construct safely.
    pub fn new_with_callback<F>(callback: F) -> Result<Self>
    where
        F: Fn() + Send + 'static,
    {
        let interrupted = Arc::new(AtomicBool::new(false));
        let locks = Arc::new(Mutex::new(Vec::<LockHandle>::new()));

        let sigint_handler = unsafe {
            // SAFETY: the closure only sets an atomic flag, which is async-signal-safe.
            register(libc::SIGINT, {
                let flag = interrupted.clone();
                move || flag.store(true, Ordering::SeqCst)
            })?
        };
        let sigterm_handler = unsafe {
            // SAFETY: the closure only sets an atomic flag, which is async-signal-safe.
            register(libc::SIGTERM, {
                let flag = interrupted.clone();
                move || flag.store(true, Ordering::SeqCst)
            })?
        };

        spawn_cleanup_worker_with_callback(interrupted.clone(), locks.clone(), callback);

        Ok(CleanupHandler {
            sigint_handler,
            sigterm_handler,
            interrupted,
            locks,
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

/// Spawns a daemon thread that polls the interrupted flag and releases all tracked locks.
fn spawn_cleanup_worker(interrupted: Arc<AtomicBool>, locks: Arc<Mutex<Vec<LockHandle>>>) {
    thread::Builder::new()
        .name("cleanup".into())
        .spawn(move || {
            loop {
                if interrupted.load(Ordering::Relaxed) {
                    if let Ok(locks) = locks.lock() {
                        for lock in locks.iter() {
                            lock.trigger_unlock();
                        }
                    }
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
        })
        .ok();
}

/// Spawns a daemon thread that polls the interrupted flag, runs the callback, then releases locks.
fn spawn_cleanup_worker_with_callback<F>(
    interrupted: Arc<AtomicBool>,
    locks: Arc<Mutex<Vec<LockHandle>>>,
    callback: F,
) where
    F: Fn() + Send + 'static,
{
    thread::Builder::new()
        .name("cleanup".into())
        .spawn(move || {
            loop {
                if interrupted.load(Ordering::Relaxed) {
                    callback();
                    if let Ok(locks) = locks.lock() {
                        for lock in locks.iter() {
                            lock.trigger_unlock();
                        }
                    }
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
        })
        .ok();
}

impl Drop for CleanupHandler {
    fn drop(&mut self) {
        unregister(self.sigint_handler);
        unregister(self.sigterm_handler);

        unsafe {
            // SAFETY: std::process::exit terminates the process immediately; it is
            // async-signal-safe and the only operation in these handlers.
            let _ = register(libc::SIGINT, move || {
                std::process::exit(128 + libc::SIGINT);
            });
            let _ = register(libc::SIGTERM, move || {
                std::process::exit(128 + libc::SIGTERM);
            });
        }
    }
}
