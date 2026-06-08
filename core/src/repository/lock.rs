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

    /// Context about the process that acquired the lock (e.g., command line arguments).
    #[serde(default)]
    context: Vec<String>,

    /// When the lock was originally acquired.
    #[serde(default)]
    creation_time: Option<DateTime<Local>>,
}

impl Lock {
    pub fn new(exclusive: bool) -> Self {
        let (hostname, username) = utils::get_system_info();
        let now = Local::now();

        Self {
            id: ID::new_random(),
            timestamp: now,
            exclusive,
            hostname: hostname.unwrap_or_default(),
            username: username.unwrap_or_default(),
            pid: std::process::id(),
            context: std::env::args().collect(),
            creation_time: Some(now),
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
            context: vec!["test".to_string()],
            creation_time: Some(timestamp),
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

    pub fn context(&self) -> &[String] {
        &self.context
    }

    pub fn creation_time(&self) -> Option<DateTime<Local>> {
        self.creation_time
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Checks if the lock is expired based on the refresh timestamp.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        let now = Local::now();
        // LOCK_EXPIRE_TIMEOUT is a compile-time constant, so from_std cannot fail.
        let expire_timeout_chrono = chrono::Duration::from_std(LOCK_EXPIRE_TIMEOUT)
            .expect("LOCK_EXPIRE_TIMEOUT is a valid Duration");

        now.signed_duration_since(self.timestamp) > expire_timeout_chrono
    }

    /// Checks if the process that acquired the lock is still alive.
    ///
    /// This only works if the lock was acquired on the same host.
    #[must_use]
    pub fn process_alive(&self) -> bool {
        let (current_hostname, _) = utils::get_system_info();
        if self.hostname.is_empty() {
            return true;
        }
        let current_hostname = match current_hostname {
            Some(h) => h,
            None => return true,
        };
        if self.hostname != current_hostname {
            // We can't know for sure if the process is alive on another host.
            return true;
        }

        #[cfg(unix)]
        {
            // On Unix, sending signal 0 to a process is a safe way to check its existence.
            unsafe { libc::kill(self.pid as libc::pid_t, 0) == 0 }
        }

        #[cfg(windows)]
        {
            use windows_sys::Win32::{
                Foundation::{CloseHandle, FALSE, STILL_ACTIVE},
                System::Threading::{
                    GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
                },
            };

            unsafe {
                let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, self.pid);
                if handle.is_null() {
                    return false;
                }
                let mut exit_code = 0;
                let res = GetExitCodeProcess(handle, &mut exit_code);
                CloseHandle(handle);

                res != FALSE && exit_code == STILL_ACTIVE as u32
            }
        }

        #[cfg(not(any(unix, windows)))]
        {
            true
        }
    }

    /// A lock is considered stale if it's expired OR if we can prove the process is dead.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        self.is_expired() || !self.process_alive()
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

                tracing::debug!(target: "repo", "Refreshing lock {}", lock.lock().id().to_short_hex(8));
                if let Err(e) = repo.refresh_lock(&lock).await {
                    ui::cli::warning!("Failed to refresh lock: {}", e);
                    tracing::warn!(target: "repo", "Failed to refresh lock: {}", e);
                }
            }
        });
    }

    pub async fn unlock(&self) {
        let _guard = self.unlock_mutex.lock().await;
        if self.alive_flag.swap(false, Ordering::SeqCst) && !self.dry_run {
            tracing::info!(target: "repo", "Releasing lock {}", self.lock.lock().id().to_short_hex(8));
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
            // Prefer the current thread's runtime (covers the common case where
            // LockHandle::drop runs during shutdown on a still-active runtime).
            // Fall back to the captured handle from when the lock was acquired.
            let handle =
                tokio::runtime::Handle::try_current().unwrap_or(self.runtime_handle.clone());

            let repo = self.repo.clone();
            let lock = self.lock.clone();
            let alive_flag = self.alive_flag.clone();
            let unlock_mutex = self.unlock_mutex.clone();

            handle.spawn(async move {
                let _guard = unlock_mutex.lock().await;
                if alive_flag.swap(false, Ordering::SeqCst) {
                    let lock_id = {
                        let lock_guard = lock.lock();
                        *lock_guard.id()
                    };

                    tracing::info!(target: "repo", "Releasing lock {} (triggered)", lock_id.to_short_hex(8));
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
    use chrono::{Duration as ChronoDuration, Local};

    use super::*;

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
