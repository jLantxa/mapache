use std::{sync::LazyLock, time::Duration};

use directories::BaseDirs;
use parking_lot::RwLock;

use crate::{
    commands::GlobalArgs,
    mapache::{
        defaults::{DEFAULT_PROGRESS_REFRESH_RATE_HZ, DEFAULT_VERBOSITY},
        vars::{REFRESH_RATE_ENVVAR, get_envvar},
    },
};

/// Base OS directories (cache, home, etc.)
pub static BASE_DIRS: LazyLock<RwLock<BaseDirs>> = LazyLock::new(|| {
    RwLock::new(
        BaseDirs::new()
            .expect("Expected to find a valid user home directory to initialize base paths"),
    )
});

/// Global arguments that can be read at any time during the execution of the program.
pub static GLOBAL_OPTS: LazyLock<RwLock<Option<GlobalOpts>>> =
    LazyLock::new(|| RwLock::new(Some(GlobalOpts::default())));

/// Global execution arguments parameters. This struct stores global configuration.
/// These values may come from the CLI, program constants, environment variables,
/// configuration file, etc.
#[derive(Debug)]
pub struct GlobalOpts {
    pub verbosity: u32,
    pub progress_refresh_interval: Duration,
}

// NOTE: Logging macros can't be used here, as they need to read the verbosity
// value from the GLOBAL_OPTS, acquiring a lock.
impl GlobalOpts {
    fn get_refresh_interval() -> Duration {
        let var_hz = get_envvar(REFRESH_RATE_ENVVAR);

        let refresh_rate_hz = match var_hz {
            Some(val) => match val.parse::<f32>() {
                Ok(hz) => {
                    if hz > 0.0 && hz <= 60.0 {
                        hz
                    } else {
                        DEFAULT_PROGRESS_REFRESH_RATE_HZ
                    }
                }
                Err(_) => DEFAULT_PROGRESS_REFRESH_RATE_HZ,
            },
            None => DEFAULT_PROGRESS_REFRESH_RATE_HZ,
        };

        calculate_refresh_interval(refresh_rate_hz)
    }

    /// Returns the global verbosity setting.
    pub fn verbosity() -> u32 {
        GLOBAL_OPTS.read().as_ref().unwrap().verbosity
    }

    /// Returns the global progress refresh interval.
    pub fn progress_refresh_interval() -> Duration {
        GLOBAL_OPTS
            .read()
            .as_ref()
            .unwrap()
            .progress_refresh_interval
    }
}

impl Default for GlobalOpts {
    fn default() -> Self {
        Self {
            verbosity: DEFAULT_VERBOSITY,
            progress_refresh_interval: Self::get_refresh_interval(),
        }
    }
}

/// Sets global options from the global args.
pub fn set_global_opts_with_args(global_args: &GlobalArgs) {
    let verbosity = if global_args.quiet {
        0
    } else if let Some(v) = global_args.verbosity {
        v
    } else {
        DEFAULT_VERBOSITY
    };

    // Default values that don't change
    let progress_refresh_interval = GlobalOpts::progress_refresh_interval();

    let new_opts = GlobalOpts {
        verbosity,
        progress_refresh_interval,
    };

    let _ = GLOBAL_OPTS.write().insert(new_opts);
}

fn calculate_refresh_interval(hz: f32) -> Duration {
    Duration::from_millis((1000.0 / hz) as u64)
}
