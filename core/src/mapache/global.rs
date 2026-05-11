use std::{
    sync::{
        LazyLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use directories::BaseDirs;

use crate::{
    commands::GlobalArgs,
    mapache::{
        defaults::{DEFAULT_PROGRESS_REFRESH_RATE_HZ, DEFAULT_VERBOSITY},
        vars::{REFRESH_RATE_ENVVAR, get_envvar},
    },
};

pub(crate) const THIS_MAPACHE_VERSION: &str = match option_env!("MAPACHE_RELEASE_BUILD") {
    Some(_) => {
        concat!("v", env!("CARGO_PKG_VERSION"))
    }
    None => concat!("v", env!("CARGO_PKG_VERSION"), "+dev"),
};

/// Base OS directories (cache, home, etc.)
pub(crate) static BASE_DIRS: LazyLock<BaseDirs> = LazyLock::new(|| {
    BaseDirs::new().expect("Expected to find a valid user home directory to initialize base paths")
});

/// Global Settings stored in Atomics for lock-free, thread-safe access.
/// This allows logging macros to check verbosity without lock contention or deadlock risks.
static VERBOSITY: AtomicU32 = AtomicU32::new(DEFAULT_VERBOSITY);
static REFRESH_INTERVAL_MS: AtomicU64 =
    AtomicU64::new((1000.0 / DEFAULT_PROGRESS_REFRESH_RATE_HZ) as u64);

pub(crate) struct GlobalOpts;

impl GlobalOpts {
    /// Returns the global verbosity setting.
    #[inline]
    pub(crate) fn verbosity() -> u32 {
        VERBOSITY.load(Ordering::Relaxed)
    }

    /// Returns the global progress refresh interval as a Duration.
    pub(crate) fn progress_refresh_interval() -> Duration {
        Duration::from_millis(REFRESH_INTERVAL_MS.load(Ordering::Relaxed))
    }

    /// Sets the global verbosity level.
    pub(crate) fn set_verbosity(verbosity: u32) {
        VERBOSITY.store(verbosity, Ordering::Relaxed);
    }

    /// Internal logic to parse refresh rate and update the atomic storage.
    fn init_refresh_interval() {
        let hz = get_envvar(REFRESH_RATE_ENVVAR)
            .and_then(|val| val.parse::<f32>().ok())
            .filter(|&hz| hz > 0.0 && hz <= 60.0)
            .unwrap_or(DEFAULT_PROGRESS_REFRESH_RATE_HZ);

        let ms = (1000.0 / hz) as u64;
        REFRESH_INTERVAL_MS.store(ms, Ordering::Relaxed);
    }
}

/// Sets global options from CLI arguments and environment variables.
/// This should be called once near the start of `main`.
pub fn set_global_opts_with_args(global_args: &GlobalArgs) {
    let verbosity = if global_args.quiet || global_args.json {
        0
    } else {
        global_args.verbosity.unwrap_or(DEFAULT_VERBOSITY)
    };

    GlobalOpts::set_verbosity(verbosity);
    GlobalOpts::init_refresh_interval();
}
