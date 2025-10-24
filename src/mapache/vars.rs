/// UI refresh rate in Hz [f32]
pub(crate) const REFRESH_RATE_ENVVAR: &str = "MAPACHE_REFRESH_RATE";

pub(crate) fn get_envvar(var: &str) -> Option<String> {
    std::env::var(var).ok()
}
