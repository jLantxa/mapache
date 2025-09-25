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

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Local};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    global::{FileType, ID},
    repository::repo::Repository,
    ui, utils,
};

// Lock self refresh period
const LOCK_REFRESH_PERIOD: std::time::Duration = Duration::from_secs(60);

// Lock timeout. This is the time a Lock must go without being refreshed to be considered
// expired. Expired locks can
pub(crate) const LOCK_EXPIRE_TIMEOUT: std::time::Duration = Duration::from_secs(5 * 60);

const _: () = {
    assert!(
        LOCK_REFRESH_PERIOD.as_secs() < 2 * LOCK_EXPIRE_TIMEOUT.as_secs(),
        "LOCK_REFRESH_PERIOD must be strictly less than LOCK_EXPIRE_TIMEOUT"
    );
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Lock {
    /// A Unique ID for the lock.
    ///
    /// The ID is also the filename of this lock when saved in the repository. It is randomly
    /// generated and has nothing to do with the serialized content because the timestamp is
    /// supposed to be refreshed periodically.
    id: ID,

    /// The last time this lock was refreshed.
    timestamp: DateTime<Local>,

    /// Exclusive flag.
    ///
    /// An exclusive lock can only be acquired when no other locks exist. A non-exclusive lock can
    /// be acquired only if no exclusive lock exists.
    exclusive: bool,

    /// The name of the host that acquired the lock.
    hostname: String,

    /// The name of the user that acquired the lock.
    username: String,

    /// The ID of the process that acquired the lock.
    pid: u32,
}

impl Lock {
    pub fn new(exclusive: bool) -> Self {
        let (hostname, username) = utils::get_system_info();

        Self {
            id: ID::new_random(),
            timestamp: Local::now(),
            exclusive,
            hostname: hostname.unwrap_or_default(),
            username: username.unwrap_or_default(),
            pid: std::process::id(),
        }
    }

    pub fn refresh(&mut self) {
        self.timestamp = Local::now();
    }

    pub fn id(&self) -> &ID {
        &self.id
    }

    pub fn timestamp(&self) -> &DateTime<Local> {
        &self.timestamp
    }

    pub fn is_exclusive(&self) -> bool {
        self.exclusive
    }

    pub fn is_expired(&self) -> bool {
        let now = Local::now();
        let expire_timeout_chrono = chrono::Duration::from_std(LOCK_EXPIRE_TIMEOUT)
            .expect("LOCK_EXPIRE_TIMEOUT should fit into chrono::Duration");

        now.signed_duration_since(self.timestamp) > expire_timeout_chrono
    }
}

pub struct LockHandle {
    repo: Arc<Repository>,
    lock: Arc<Mutex<Lock>>,
    alive_flag: Arc<AtomicBool>,
}

impl LockHandle {
    pub fn new(repo: Arc<Repository>, lock: Arc<Mutex<Lock>>) -> Self {
        let handle = Self {
            repo: repo.clone(),
            lock,
            alive_flag: Arc::new(AtomicBool::new(true)),
        };

        handle.start_refresh_handler();
        handle
    }

    fn start_refresh_handler(&self) {
        let repo_clone = self.repo.clone();
        let lock_clone = self.lock.clone();
        let alive_flag_clone = self.alive_flag.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(LOCK_REFRESH_PERIOD);

                if !alive_flag_clone.load(Ordering::Relaxed) {
                    return;
                }

                if repo_clone.refresh_lock(&lock_clone).is_err() {
                    ui::cli::warning!("Failed to refresh lock");
                }
            }
        });
    }

    pub fn unlock(&self) {
        // Changed to &self as it's not strictly a mutable operation
        self.alive_flag.store(false, Ordering::SeqCst);
        if let Err(e) = self.repo.delete_file(FileType::Lock, self.lock.lock().id()) {
            // Log the error instead of returning it.
            ui::cli::warning!("Failed to delete lock file: {e}");
        }
    }
}

impl Drop for LockHandle {
    fn drop(&mut self) {
        self.unlock();
    }
}
