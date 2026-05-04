use std::sync::{
    Arc, Mutex, Once,
    atomic::{AtomicBool, Ordering},
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

static CLEANUP_ONCE: Once = Once::new();

impl CleanupHandler {
    /// Register callbacks for the SIGINT and SIGTERM signals.
    pub fn new() -> Result<Self> {
        let interrupted = Arc::new(AtomicBool::new(false));
        let locks = Arc::new(Mutex::new(Vec::<LockHandle>::new()));

        let clone_for_sigint = interrupted.clone();
        let locks_clone = locks.clone();
        let sigint_handler = unsafe {
            // SAFETY: register is a wrapper around signal handlers. The closure
            // provided is thread-safe and only performs atomic operations and
            // non-blocking mutex locks, which is safe in this context.
            register(libc::SIGINT, move || {
                clone_for_sigint.store(true, Ordering::SeqCst);
                if let Ok(locks) = locks_clone.lock() {
                    for lock in locks.iter() {
                        lock.trigger_unlock();
                    }
                }
            })?
        };

        let clone_for_sigterm = Arc::clone(&interrupted);
        let locks_clone = locks.clone();
        let sigterm_handler = unsafe {
            register(libc::SIGTERM, move || {
                clone_for_sigterm.store(true, Ordering::SeqCst);
                if let Ok(locks) = locks_clone.lock() {
                    for lock in locks.iter() {
                        lock.trigger_unlock();
                    }
                }
            })?
        };

        Ok(CleanupHandler {
            sigint_handler,
            sigterm_handler,
            interrupted,
            locks,
        })
    }

    /// Register callbacks for the SIGINT and SIGTERM signals with an immediate UI callback.
    pub fn new_with_callback<F>(callback: F) -> Result<Self>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let interrupted = Arc::new(AtomicBool::new(false));
        let locks = Arc::new(Mutex::new(Vec::<LockHandle>::new()));
        let callback = Arc::new(callback);

        let clone_for_sigint = interrupted.clone();
        let locks_clone = locks.clone();
        let cb_sigint = callback.clone();
        let sigint_handler = unsafe {
            // SAFETY: register is a wrapper around signal handlers. The closure
            // provided is thread-safe and only performs atomic operations and
            // non-blocking mutex locks, which is safe in this context.
            register(libc::SIGINT, move || {
                clone_for_sigint.store(true, Ordering::SeqCst);
                CLEANUP_ONCE.call_once(|| {
                    cb_sigint();
                    if let Ok(locks) = locks_clone.lock() {
                        for lock in locks.iter() {
                            lock.trigger_unlock();
                        }
                    }
                });
            })?
        };

        let clone_for_sigterm = interrupted.clone();
        let locks_clone = locks.clone();
        let cb_sigterm = callback.clone();
        let sigterm_handler = unsafe {
            register(libc::SIGTERM, move || {
                clone_for_sigterm.store(true, Ordering::SeqCst);
                CLEANUP_ONCE.call_once(|| {
                    cb_sigterm();
                    if let Ok(locks) = locks_clone.lock() {
                        for lock in locks.iter() {
                            lock.trigger_unlock();
                        }
                    }
                });
            })?
        };

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

impl Drop for CleanupHandler {
    fn drop(&mut self) {
        // Unregister previous handlers to drop the callbacks
        unregister(self.sigint_handler);
        unregister(self.sigterm_handler);

        // Register default termination callbacks
        unsafe {
            let _ = register(libc::SIGINT, move || {
                std::process::exit(128 + libc::SIGINT);
            });
            let _ = register(libc::SIGTERM, move || {
                std::process::exit(128 + libc::SIGTERM);
            });
        }
    }
}
