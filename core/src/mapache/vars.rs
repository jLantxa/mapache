/// UI refresh rate in Hz [f32]
pub(crate) const REFRESH_RATE_ENVVAR: &str = "MAPACHE_REFRESH_RATE";

/// Filesystem stat concurrency [usize]
pub(crate) const FS_STAT_CONCURRENCY_ENVVAR: &str = "MAPACHE_FS_STAT_CONCURRENCY";

/// Reads an environment variable and returns its string value or None if the
/// variable is not defined or cannot be read.
pub(crate) fn get_envvar(var: &str) -> Option<String> {
    std::env::var(var).ok()
}
