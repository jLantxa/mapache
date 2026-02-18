use anyhow::Result;
use signal_hook_registry::{SigId, register, unregister};
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct CleanupHandler {
    sigint_handler: SigId,
    sigterm_handler: SigId,
    pub interrupted: Arc<AtomicBool>,
}

static CLEANUP_ONCE: Once = Once::new();

impl CleanupHandler {
    /// Register callbacks for the SIGINT and SIGTERM signals.
    pub fn new() -> Result<Self> {
        let interrupted = Arc::new(AtomicBool::new(false));

        let clone_for_sigint = interrupted.clone();
        let sigint_handler = unsafe {
            register(libc::SIGINT, move || {
                clone_for_sigint.store(true, Ordering::SeqCst);
            })?
        };

        let clone_for_sigterm = Arc::clone(&interrupted);
        let sigterm_handler = unsafe {
            register(libc::SIGTERM, move || {
                clone_for_sigterm.store(true, Ordering::SeqCst);
            })?
        };

        Ok(CleanupHandler {
            sigint_handler,
            sigterm_handler,
            interrupted,
        })
    }

    /// Register callbacks for the SIGINT and SIGTERM signals with an immediate UI callback.
    pub fn new_with_callback<F>(callback: F) -> Result<Self>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let interrupted = Arc::new(AtomicBool::new(false));
        let callback = Arc::new(callback);

        let clone_for_sigint = interrupted.clone();
        let cb_sigint = callback.clone();
        let sigint_handler = unsafe {
            register(libc::SIGINT, move || {
                clone_for_sigint.store(true, Ordering::SeqCst);
                CLEANUP_ONCE.call_once(|| {
                    cb_sigint();
                });
            })?
        };

        let clone_for_sigterm = interrupted.clone();
        let cb_sigterm = callback.clone();
        let sigterm_handler = unsafe {
            register(libc::SIGTERM, move || {
                clone_for_sigterm.store(true, Ordering::SeqCst);
                CLEANUP_ONCE.call_once(|| {
                    cb_sigterm();
                });
            })?
        };

        Ok(CleanupHandler {
            sigint_handler,
            sigterm_handler,
            interrupted,
        })
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted.load(Ordering::SeqCst)
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
