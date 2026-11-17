/// UI refresh rate in Hz [f32]
pub(crate) const REFRESH_RATE_ENVVAR: &str = "MAPACHE_REFRESH_RATE";

/// Repository username
pub(crate) const USERNAME_ENVVAR: &str = "MAPACHE_USERNAME";

/// Repository password
pub(crate) const PASSWORD_ENVVAR: &str = "MAPACHE_PASSWORD";

/// Command whose standard output is a JSON object with the repository
/// credentials (`{"username": ..., "password": ...}`).
pub(crate) const PASSWORD_COMMAND_ENVVAR: &str = "MAPACHE_PASSWORD_COMMAND";

/// Debug log output directory
pub(crate) const DEBUG_PATH_ENVVAR: &str = "MAPACHE_DEBUG_PATH";

/// Debug log level (trace, debug, info, warn, error). Defaults to debug.
pub(crate) const DEBUG_LEVEL_ENVVAR: &str = "MAPACHE_DEBUG_LEVEL";

/// Maximum number of Tokio blocking threads.
pub(crate) const MAX_BLOCKING_THREADS_ENVVAR: &str = "MAPACHE_MAX_BLOCKING_THREADS";

/// Reads an environment variable and returns its string value or None if the
/// variable is not defined or cannot be read.
pub(crate) fn get_envvar(var: &str) -> Option<String> {
    std::env::var(var).ok()
}
