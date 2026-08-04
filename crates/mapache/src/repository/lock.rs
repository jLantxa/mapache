use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Local};
use parking_lot::Mutex;
use rand::{RngExt, rng};
use serde::{Deserialize, Serialize};

use crate::{
    backend::{Handle, StorageHint},
    common::error::{MapacheError, Result},
    common::{ContentIdType, ID, SaveID},
    repository::repo::{LOCKS_DIR, Repository},
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
            // If it returns -1 but errno is EPERM, the process still exists (we just don't have permission to signal it).
            unsafe {
                // SAFETY: FFI call to kill(pid, 0) to check process existence.
                let ret = libc::kill(self.pid as libc::pid_t, 0);
                ret == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
            }
        }

        #[cfg(windows)]
        {
            use windows_sys::Win32::{
                Foundation::{CloseHandle, FALSE, STILL_ACTIVE},
                System::Threading::{
                    GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
                },
            };

            // SAFETY: OpenProcess/GetExitCodeProcess/CloseHandle are Windows
            // FFI calls with valid parameters. Null handle is checked.
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
    alive: bool,
}

impl LockHandle {
    pub fn new(repo: Arc<Repository>, lock: Arc<Mutex<Lock>>, alive: bool) -> Self {
        let handle = Self {
            repo: repo.clone(),
            lock,
            alive_flag: Arc::new(AtomicBool::new(true)),
            unlock_mutex: Arc::new(tokio::sync::Mutex::new(())),
            runtime_handle: tokio::runtime::Handle::current(),
            alive,
        };

        if alive {
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

                let lock_id = { lock.lock().id().to_short_hex(8) };
                tracing::debug!(target: "repo", "Refreshing lock {}", lock_id);
                if let Err(e) = repo.refresh_lock(&lock).await {
                    ui::cli::warning!("Failed to refresh lock: {}", e);
                    tracing::warn!(target: "repo", "Failed to refresh lock: {}", e);
                }
            }
        });
    }

    pub async fn unlock(&self) {
        let _guard = self.unlock_mutex.lock().await;
        if self.alive_flag.swap(false, Ordering::SeqCst) && self.alive {
            let lock_id = { self.lock.lock().id().to_short_hex(8) };
            tracing::info!(target: "repo", "Releasing lock {}", lock_id);
            self.perform_delete().await;
        }
    }

    async fn perform_delete(&self) {
        Self::delete_lock_file(&self.repo, &self.lock).await;
    }

    async fn delete_lock_file(repo: &Repository, lock: &Mutex<Lock>) {
        // Get the ID without holding the lock across an await point
        let lock_id = {
            let lock_guard = lock.lock();
            *lock_guard.id()
        };

        if let Err(e) = repo.delete_file(ContentIdType::Lock, &lock_id, None).await {
            tracing::warn!(target: "repo", "Failed to delete lock {}: {e}", lock_id.to_short_hex(8));
        }
    }

    pub fn trigger_unlock(&self) {
        if self.alive_flag.load(Ordering::SeqCst) && self.alive {
            // Prefer the current thread's runtime (covers the common case where
            // LockHandle::drop runs during shutdown on a still-active runtime).
            // Fall back to the captured handle from when the lock was acquired.
            let handle =
                tokio::runtime::Handle::try_current().unwrap_or(self.runtime_handle.clone());

            let repo = self.repo.clone();
            let lock = self.lock.clone();
            let alive_flag = self.alive_flag.clone();
            let unlock_mutex = self.unlock_mutex.clone();

            // Attempt to spawn the cleanup as an async task.
            // If spawn fails (runtime shutting down), log the issue.
            // The lock will be cleaned up by the next process via the stale-lock
            // mechanism (LOCK_EXPIRE_TIMEOUT).
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handle.spawn(async move {
                    let _guard = unlock_mutex.lock().await;
                    if alive_flag.swap(false, Ordering::SeqCst) {
                        let lock_id = {
                            let lock_guard = lock.lock();
                            *lock_guard.id()
                        };

                        tracing::info!(target: "repo", "Releasing lock {} (triggered)", lock_id.to_short_hex(8));
                        Self::delete_lock_file(&repo, &lock).await;
                    }
                });
            })) {
                Ok(_) => {}
                Err(_) => {
                    self.alive_flag.store(false, Ordering::SeqCst);
                    let lock_id = { self.lock.lock().id().to_short_hex(8) };
                    tracing::warn!(target: "repo", "Runtime shutdown; lock {} may remain stale (will be cleaned up on expiry)", lock_id);
                }
            }
        }
    }
}

impl Drop for LockHandle {
    fn drop(&mut self) {
        self.trigger_unlock();
    }
}

impl Repository {
    /// Try to acquire a lock with a retry deadline
    pub(crate) async fn try_acquire_lock_with_retry(
        &self,
        exclusive: bool,
        retry_duration: Option<chrono::Duration>,
    ) -> Result<Arc<Mutex<Lock>>> {
        let start_time = Instant::now();

        const MIN_BASE_WAIT_INTERVAL_MS: i64 = 5 * 1000;
        const MAX_BASE_WAIT_INTERVAL_MS: i64 = 60 * 1000;
        const MAX_JITTER_MS: i64 = 1000;

        let mut base_wait_interval_ms = MIN_BASE_WAIT_INTERVAL_MS;

        loop {
            match self.try_acquire_lock_once(exclusive).await {
                Ok(lock) => return Ok(lock),

                Err(e) => {
                    let timeout = match retry_duration {
                        Some(t) => t,
                        None => return Err(e),
                    };

                    if start_time.elapsed() >= timeout.to_std().unwrap_or_default() {
                        return Err(MapacheError::LockExpired(
                            "timeout acquiring repository lock".to_string(),
                        ));
                    }

                    let mut rng = rng();
                    let jitter_millis = rng.random_range(0..MAX_JITTER_MS);
                    let mean_wait_interval =
                        chrono::Duration::milliseconds(base_wait_interval_ms - (MAX_JITTER_MS / 2));
                    let wait_time =
                        mean_wait_interval + chrono::Duration::milliseconds(jitter_millis);
                    base_wait_interval_ms =
                        std::cmp::min(MAX_BASE_WAIT_INTERVAL_MS, 2 * base_wait_interval_ms);

                    ui::cli::warning!(
                        "The repository is locked by another process. Waiting {:.0?} seconds before retrying...",
                        wait_time.as_seconds_f32()
                    );

                    tokio::time::sleep(
                        wait_time.to_std().map_err(|e| {
                            MapacheError::Repo(format!("duration out of range: {e}"))
                        })?,
                    )
                    .await;
                }
            }
        }
    }

    /// Try to acquire a lock just once without retrying
    async fn try_acquire_lock_once(&self, exclusive: bool) -> Result<Arc<Mutex<Lock>>> {
        self.backend().create_dir(&PathBuf::from(LOCKS_DIR)).await?;
        let new_lock = Arc::new(Mutex::new(Lock::new(exclusive)));

        let new_lock_id = *new_lock.lock().id();

        self.save_lock(&new_lock)
            .await
            .map_err(|e| MapacheError::Backend(format!("failed to write new lock file: {e}")))?;

        let all_locks = match self.get_locks().await {
            Ok(locks) => locks,
            Err(e) => {
                if let Err(del_err) = self
                    .delete_file(ContentIdType::Lock, &new_lock_id, None)
                    .await
                {
                    tracing::warn!(target: "repo", "Failed to clean up lock {} after get_locks error: {del_err}", new_lock_id.to_short_hex(8));
                }
                return Err(e);
            }
        };

        for lock in all_locks {
            // Skip the lock we just wrote
            if lock.id() == &new_lock_id {
                continue;
            }

            // Clean up stale locks from other processes.
            // A lock is stale if it's expired OR if the process is dead on the same host.
            if lock.is_stale() {
                if let Err(e) = self.delete_file(ContentIdType::Lock, lock.id(), None).await {
                    tracing::warn!(target: "repo", "Failed to delete stale lock {}: {e}", lock.id().to_short_hex(8));
                }
                continue;
            }

            if exclusive || lock.is_exclusive() {
                // A race condition occurred, or a conflict was already present.
                // The NEWLY written lock must be cleaned up and the attempt must fail.
                if let Err(e) = self
                    .delete_file(ContentIdType::Lock, &new_lock_id, None)
                    .await
                {
                    tracing::warn!(target: "repo", "Failed to clean up conflicting lock {}: {e}", new_lock_id.to_short_hex(8));
                }

                let info = format!(
                    "conflict detected with existing lock.\n\
                     ID:      {}\n\
                     Host:    {}\n\
                     User:    {}\n\
                     PID:     {}\n\
                     Started: {}\n\
                     Context: {}",
                    lock.id().to_short_hex(4),
                    lock.hostname(),
                    lock.username(),
                    lock.pid(),
                    lock.creation_time()
                        .map(|t| utils::pretty_print_timestamp(&t, None))
                        .unwrap_or_else(|| "unknown".to_string()),
                    lock.context().join(" ")
                );

                return Err(MapacheError::Locked(info));
            }
        }

        Ok(new_lock)
    }

    pub(crate) async fn save_lock(&self, lock: &Arc<Mutex<Lock>>) -> Result<()> {
        let (lock_id, lock_bytes) = {
            let lock_guard = lock.lock();
            let id = *lock_guard.id();
            let json = serde_json::to_string(&*lock_guard)?;
            (id, json.into_bytes())
        };

        self.save_file(
            &SaveID::WithID(lock_id),
            &lock_bytes,
            StorageHint {
                file_type: ContentIdType::Lock,
                is_metadata: true,
            },
            None,
        )
        .await?;

        Ok(())
    }

    pub async fn refresh_lock(&self, lock: &Arc<Mutex<Lock>>) -> Result<()> {
        lock.lock().refresh();
        self.save_lock(lock).await
    }

    /// Get all locks in the repository. If a lock file cannot be read, decoded
    /// or deserialized, it will be ignored.
    pub async fn get_locks(&self) -> Result<Vec<Lock>> {
        let all_lock_paths = self.list_files(ContentIdType::Lock).await?;

        let mut locks = Vec::new();
        for path in all_lock_paths {
            let Some(data) = self
                .backend()
                .read(
                    &Handle::new_with_hint(&path, ContentIdType::Lock, true),
                    0,
                    0,
                )
                .await
                .ok()
            else {
                continue;
            };
            let Some(decoded) = self.secure_storage().decode_owned(data).ok() else {
                continue;
            };
            if let Ok(lock) = serde_json::from_slice::<Lock>(&decoded) {
                locks.push(lock);
            }
        }

        Ok(locks)
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
    fn test_lock_refresh_resets_expiry() {
        let past = Local::now() - ChronoDuration::hours(2);
        let mut lock = Lock::new_for_test(true, past);
        assert!(lock.is_expired());
        lock.refresh();
        assert!(!lock.is_expired());
    }

    #[test]
    fn test_lock_not_stale_when_fresh() {
        let lock = Lock::new(true);
        // A fresh lock should not be stale (assuming process is alive, which it is for tests)
        assert!(!lock.is_stale());
    }

    #[test]
    fn test_lock_stale_when_expired() {
        let past = Local::now() - ChronoDuration::hours(2);
        let lock = Lock::new_for_test(true, past);
        assert!(lock.is_stale());
    }

    #[test]
    fn test_lock_serialization_roundtrip() {
        let lock = Lock::new(true);
        let json = serde_json::to_string(&lock).unwrap();
        let parsed: Lock = serde_json::from_str(&json).unwrap();
        assert_eq!(lock.id(), parsed.id());
        assert_eq!(lock.is_exclusive(), parsed.is_exclusive());
        assert_eq!(lock.pid(), parsed.pid());
        assert_eq!(lock.hostname(), parsed.hostname());
        assert_eq!(lock.username(), parsed.username());
    }

    #[test]
    fn test_lock_id_uniqueness() {
        let lock1 = Lock::new(true);
        let lock2 = Lock::new(true);
        assert_ne!(lock1.id(), lock2.id());
    }
}
