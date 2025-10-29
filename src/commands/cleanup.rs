use anyhow::Result;
use parking_lot::Mutex;
use signal_hook::SigId;
use signal_hook_registry::{self, register, unregister};
use std::sync::Arc;

pub struct CleanupHandler {
    sigint_handler: SigId,
    sigterm_handler: SigId,
}

impl CleanupHandler {
    /// Register callbacks for the SIGINT and SIGTERM signals. These callbacks with
    /// be unregistered when the handler is dropped.
    pub fn new<F: FnMut() + Send + 'static>(cleanup_fn: F) -> Result<Self> {
        let shared_cleanup = Arc::new(Mutex::new(cleanup_fn));

        let clone_for_sigint = shared_cleanup.clone();
        let sigint_handler = unsafe {
            register(libc::SIGINT, move || {
                clone_for_sigint.lock()();
                std::process::exit(128 + libc::SIGINT);
            })?
        };

        let clone_for_sigterm = Arc::clone(&shared_cleanup);
        let sigterm_handler = unsafe {
            register(libc::SIGTERM, move || {
                clone_for_sigterm.lock()();
                std::process::exit(128 + libc::SIGTERM);
            })?
        };

        Ok(CleanupHandler {
            sigint_handler,
            sigterm_handler,
        })
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
