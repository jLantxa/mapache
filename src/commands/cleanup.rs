// mapache is a secure, de-duplicating, incremental backup tool.
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use anyhow::Result;
use parking_lot::Mutex;
use signal_hook::SigId;
use signal_hook_registry::{self, register, unregister};
use std::sync::Arc;

use crate::ui;

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
                ui::cli::log!("Process interrupted: cleaning up...");
                clone_for_sigint.lock()();
                std::process::exit(128 + libc::SIGINT);
            })?
        };

        let clone_for_sigterm = Arc::clone(&shared_cleanup);
        let sigterm_handler = unsafe {
            register(libc::SIGTERM, move || {
                ui::cli::log!("Process terminated: cleaning up...");
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
