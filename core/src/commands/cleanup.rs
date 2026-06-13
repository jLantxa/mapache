use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, OwnedHandle};

use anyhow::Result;
use signal_hook::consts::signal::{SIGINT, SIGTERM};

use crate::repository::lock::LockHandle;

#[cfg(unix)]
use signal_hook::iterator::{Handle, Signals};

// ---------------------------------------------------------------------------
// Windows pipe-based signal wake (zero polling)
// ---------------------------------------------------------------------------
#[cfg(windows)]
mod pipe {
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};

    use anyhow::Result;
    use windows_sys::Win32::{
        Foundation::HANDLE, Storage::FileSystem::ReadFile, System::Pipes::CreatePipe,
    };

    /// Owns the read end of an anonymous pipe. `wait()` blocks until data is
    /// available.
    pub struct WakePipe {
        read_end: OwnedHandle,
    }

    impl WakePipe {
        pub fn new() -> Result<(Self, OwnedHandle)> {
            unsafe {
                let mut r = std::ptr::null_mut();
                let mut w = std::ptr::null_mut();
                if CreatePipe(&mut r, &mut w, std::ptr::null(), 0) == 0 {
                    anyhow::bail!("CreatePipe failed");
                }
                Ok((
                    WakePipe {
                        read_end: OwnedHandle::from_raw_handle(r),
                    },
                    OwnedHandle::from_raw_handle(w),
                ))
            }
        }

        /// Blocks until a byte is available. Returns `true` if woken by a
        /// signal, `false` on EOF (write end closed → clean shutdown).
        pub fn wait(&self) -> bool {
            unsafe {
                let mut byte: u8 = 0;
                let mut n: u32 = 0;
                ReadFile(
                    self.read_end.as_raw_handle() as HANDLE,
                    &mut byte,
                    1,
                    &mut n,
                    std::ptr::null_mut(),
                ) != 0
                    && n > 0
            }
        }
    }
}

#[cfg(windows)]
use pipe::WakePipe;

#[cfg(windows)]
use signal_hook::SigId;
#[cfg(windows)]
use signal_hook_registry::register as register_signal;

#[cfg(windows)]
use windows_sys::Win32::{Foundation::HANDLE, Storage::FileSystem::WriteFile};

use signal_hook_registry::register;

// ---------------------------------------------------------------------------
// Windows: HANDLE is *mut c_void (!Send).  Wrapper for use in Send closures.
// ---------------------------------------------------------------------------
#[cfg(windows)]
#[derive(Clone, Copy)]
struct WakeHandle(HANDLE);

// SAFETY: WakeHandle wraps a raw pipe write-end HANDLE. Moving the handle
// between threads is safe (kernel handle, no ownership metadata). Sharing is
// safe because WriteFile on an anonymous pipe is thread-safe (kernel
// serialises writes) and the handle is kept alive by CleanupHandler's
// OwnedHandle for the entire registration lifetime.
#[cfg(windows)]
unsafe impl Send for WakeHandle {}
#[cfg(windows)]
unsafe impl Sync for WakeHandle {}

#[cfg(windows)]
impl WakeHandle {
    fn write_byte(&self) {
        unsafe {
            let byte: u8 = 1;
            WriteFile(self.0, &byte, 1, std::ptr::null_mut(), std::ptr::null_mut());
        }
    }
}

// ---------------------------------------------------------------------------
// CleanupHandler
// ---------------------------------------------------------------------------
pub struct CleanupHandler {
    #[cfg(unix)]
    handle: Handle,
    #[cfg(windows)]
    _write_end: OwnedHandle, // Kept alive for RAII: drop closes the pipe write-end, waking the worker thread
    #[cfg(windows)]
    sigint_id: SigId,
    #[cfg(windows)]
    sigterm_id: SigId,
    pub interrupted: Arc<AtomicBool>,
    locks: Arc<Mutex<Vec<LockHandle>>>,
}

impl CleanupHandler {
    pub fn new() -> Result<Self> {
        let interrupted = Arc::new(AtomicBool::new(false));
        let locks = Arc::new(Mutex::new(Vec::<LockHandle>::new()));

        #[cfg(unix)]
        {
            let signals = Signals::new([SIGINT, SIGTERM])?;
            let handle = signals.handle();
            spawn_cleanup_worker(signals, interrupted.clone(), locks.clone());
            Ok(CleanupHandler {
                handle,
                interrupted,
                locks,
            })
        }

        #[cfg(windows)]
        {
            let (wake, write_end) = WakePipe::new()?;
            let handle = write_end.as_raw_handle();
            // SAFETY: signal-hook-registry::register requires unsafe because
            // signal handlers run in async-signal context. Our handler only
            // writes one byte to a pipe (async-signal-safe) and sets an
            // AtomicBool. The raw HANDLE is kept valid by `_write_end`.
            let sigint_id =
                unsafe { register_signal(SIGINT, write_signal(handle, Arc::clone(&interrupted)))? };
            let sigterm_id = unsafe {
                register_signal(SIGTERM, write_signal(handle, Arc::clone(&interrupted)))?
            };
            spawn_cleanup_worker(wake, interrupted.clone(), locks.clone());
            Ok(CleanupHandler {
                _write_end: write_end,
                sigint_id,
                sigterm_id,
                interrupted,
                locks,
            })
        }
    }

    pub fn new_with_callback<F>(callback: F) -> Result<Self>
    where
        F: Fn() + Send + 'static,
    {
        let interrupted = Arc::new(AtomicBool::new(false));
        let locks = Arc::new(Mutex::new(Vec::<LockHandle>::new()));

        #[cfg(unix)]
        {
            let signals = Signals::new([SIGINT, SIGTERM])?;
            let handle = signals.handle();
            spawn_cleanup_worker_with_callback(
                signals,
                interrupted.clone(),
                locks.clone(),
                callback,
            );
            Ok(CleanupHandler {
                handle,
                interrupted,
                locks,
            })
        }

        #[cfg(windows)]
        {
            let (wake, write_end) = WakePipe::new()?;
            let handle = write_end.as_raw_handle();
            // SAFETY: see the identical block in `new()`.
            let sigint_id =
                unsafe { register_signal(SIGINT, write_signal(handle, Arc::clone(&interrupted)))? };
            let sigterm_id = unsafe {
                register_signal(SIGTERM, write_signal(handle, Arc::clone(&interrupted)))?
            };
            spawn_cleanup_worker_with_callback(wake, interrupted.clone(), locks.clone(), callback);
            Ok(CleanupHandler {
                _write_end: write_end,
                sigint_id,
                sigterm_id,
                interrupted,
                locks,
            })
        }
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

// ---------------------------------------------------------------------------
// Windows: signal handler writes one byte to wake the pipe
// ---------------------------------------------------------------------------
#[cfg(windows)]
fn write_signal(
    write_end: HANDLE,
    interrupted: Arc<AtomicBool>,
) -> impl Fn() + Sync + Send + 'static {
    let h = WakeHandle(write_end);
    move || {
        interrupted.store(true, Ordering::SeqCst);
        h.write_byte();
    }
}

// ---------------------------------------------------------------------------
// Cleanup workers – Unix
// ---------------------------------------------------------------------------
#[cfg(unix)]
fn spawn_cleanup_worker(
    mut signals: Signals,
    interrupted: Arc<AtomicBool>,
    locks: Arc<Mutex<Vec<LockHandle>>>,
) {
    thread::Builder::new()
        .name("cleanup".into())
        .spawn(move || {
            for signal in &mut signals {
                if matches!(signal, SIGINT | SIGTERM) {
                    interrupted.store(true, Ordering::SeqCst);
                    if let Ok(locks) = locks.lock() {
                        for lock in locks.iter() {
                            lock.trigger_unlock();
                        }
                    }
                    return;
                }
            }
        })
        .ok();
}

#[cfg(unix)]
fn spawn_cleanup_worker_with_callback<F>(
    mut signals: Signals,
    interrupted: Arc<AtomicBool>,
    locks: Arc<Mutex<Vec<LockHandle>>>,
    callback: F,
) where
    F: Fn() + Send + 'static,
{
    thread::Builder::new()
        .name("cleanup".into())
        .spawn(move || {
            for signal in &mut signals {
                if matches!(signal, SIGINT | SIGTERM) {
                    interrupted.store(true, Ordering::SeqCst);
                    callback();
                    if let Ok(locks) = locks.lock() {
                        for lock in locks.iter() {
                            lock.trigger_unlock();
                        }
                    }
                    return;
                }
            }
        })
        .ok();
}

// ---------------------------------------------------------------------------
// Cleanup workers – Windows (pipe-based, no polling)
// ---------------------------------------------------------------------------
#[cfg(windows)]
fn spawn_cleanup_worker(
    wake: WakePipe,
    interrupted: Arc<AtomicBool>,
    locks: Arc<Mutex<Vec<LockHandle>>>,
) {
    thread::Builder::new()
        .name("cleanup".into())
        .spawn(move || {
            if wake.wait() {
                interrupted.store(true, Ordering::SeqCst);
                if let Ok(locks) = locks.lock() {
                    for lock in locks.iter() {
                        lock.trigger_unlock();
                    }
                }
            }
        })
        .ok();
}

#[cfg(windows)]
fn spawn_cleanup_worker_with_callback<F>(
    wake: WakePipe,
    interrupted: Arc<AtomicBool>,
    locks: Arc<Mutex<Vec<LockHandle>>>,
    callback: F,
) where
    F: Fn() + Send + 'static,
{
    thread::Builder::new()
        .name("cleanup".into())
        .spawn(move || {
            if wake.wait() {
                interrupted.store(true, Ordering::SeqCst);
                callback();
                if let Ok(locks) = locks.lock() {
                    for lock in locks.iter() {
                        lock.trigger_unlock();
                    }
                }
            }
        })
        .ok();
}

// ---------------------------------------------------------------------------
// Drop – clean shutdown
// ---------------------------------------------------------------------------
impl Drop for CleanupHandler {
    fn drop(&mut self) {
        #[cfg(unix)]
        self.handle.close();

        #[cfg(windows)]
        {
            signal_hook::low_level::unregister(self.sigint_id);
            signal_hook::low_level::unregister(self.sigterm_id);
        }

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
