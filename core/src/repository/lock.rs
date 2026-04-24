use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Local};
use colored::Colorize;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    mapache::{ContentIdType, ID},
    repository::repo::Repository,
    ui, utils,
};

// Lock self refresh period
const LOCK_REFRESH_PERIOD: std::time::Duration = Duration::from_secs(3 * 60);

// Lock timeout. This is the time a Lock must go without being refreshed to be considered
// expired. Expired locks can
pub(crate) const LOCK_EXPIRE_TIMEOUT: std::time::Duration = Duration::from_secs(10 * 60);

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

    /// A constructor only visible for tests. Allows setting a custom timestamp.
    #[cfg(test)]
    pub fn new_for_test(exclusive: bool, timestamp: DateTime<Local>) -> Self {
        let (hostname, username) = utils::get_system_info();

        Self {
            id: ID::new_random(),
            timestamp,
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

#[derive(Clone)]
pub struct LockHandle {
    repo: Arc<Repository>,
    lock: Arc<Mutex<Lock>>,
    alive_flag: Arc<AtomicBool>,
    unlock_mutex: Arc<tokio::sync::Mutex<()>>,
    runtime_handle: tokio::runtime::Handle,
    dry_run: bool,
}

impl LockHandle {
    pub fn new(repo: Arc<Repository>, lock: Arc<Mutex<Lock>>, dry_run: bool) -> Self {
        let handle = Self {
            repo: repo.clone(),
            lock,
            alive_flag: Arc::new(AtomicBool::new(true)),
            unlock_mutex: Arc::new(tokio::sync::Mutex::new(())),
            runtime_handle: tokio::runtime::Handle::current(),
            dry_run,
        };

        if !dry_run {
            handle.start_refresh_handler();
        }

        handle
    }

    fn start_refresh_handler(&self) {
        let repo = self.repo.clone();
        let lock = self.lock.clone();
        let alive_flag = self.alive_flag.clone();

        self.runtime_handle.spawn(async move {
            loop {
                tokio::time::sleep(LOCK_REFRESH_PERIOD).await;

                if !alive_flag.load(Ordering::Relaxed) {
                    break;
                }

                if let Err(e) = repo.refresh_lock(&lock).await {
                    ui::cli::warning!("Failed to refresh lock: {}", e);
                }
            }
        });
    }

    pub async fn unlock(&self) {
        let _guard = self.unlock_mutex.lock().await;
        if self.alive_flag.swap(false, Ordering::SeqCst) && !self.dry_run {
            self.perform_delete().await;
        }
    }

    async fn perform_delete(&self) {
        // Get the ID without holding the lock across an await point
        let lock_id = {
            let lock_guard = self.lock.lock();
            *lock_guard.id()
        };

        let _ = self
            .repo
            .delete_file(ContentIdType::Lock, &lock_id, None)
            .await;
    }

    pub fn trigger_unlock(&self) {
        if self.alive_flag.load(Ordering::SeqCst) && !self.dry_run {
            let repo = self.repo.clone();
            let lock = self.lock.clone();
            let alive_flag = self.alive_flag.clone();
            let unlock_mutex = self.unlock_mutex.clone();

            self.runtime_handle.spawn(async move {
                let _guard = unlock_mutex.lock().await;
                if alive_flag.swap(false, Ordering::SeqCst) {
                    let lock_id = {
                        let lock_guard = lock.lock();
                        *lock_guard.id()
                    };

                    let _ = repo.delete_file(ContentIdType::Lock, &lock_id, None).await;
                }
            });
        }
    }
}

impl Drop for LockHandle {
    fn drop(&mut self) {
        self.trigger_unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, Local};

    #[test]
    fn test_lock_basic_properties() {
        let lock = Lock::new(true);
        assert!(lock.exclusive);
        assert_eq!(lock.pid, std::process::id());
    }

    #[test]
    fn test_lock_expiration() {
        let now = Local::now();
        let mut lock = Lock::new(false);

        // New lock should not be expired
        assert!(!lock.is_expired());

        let expire_threshold = ChronoDuration::from_std(LOCK_EXPIRE_TIMEOUT).unwrap();

        // Lock within expiration threshold (e.g., half the timeout)
        lock.timestamp = now - (expire_threshold / 2);
        assert!(!lock.is_expired());

        // Lock past expiration threshold
        lock.timestamp = now - expire_threshold - ChronoDuration::seconds(10);
        assert!(lock.is_expired());

        // Refresh should make it not expired
        lock.refresh();
        assert!(!lock.is_expired());
    }

    #[test]
    fn test_lock_new_for_test() {
        let past = Local::now() - ChronoDuration::hours(1);
        let lock = Lock::new_for_test(true, past);
        assert_eq!(*lock.timestamp(), past);
        assert!(lock.is_expired());
    }
}
