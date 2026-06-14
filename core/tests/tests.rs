#![cfg(test)]

use std::sync::LazyLock;

mod integration_tests;
pub mod synthetic;

/// Environment variable to set the --quiet global option during testing.
const MAPACHE_TEST_VERBOSE: &str = "MAPACHE_TEST_VERBOSE";

static TEST_QUIET: LazyLock<bool> = LazyLock::new(|| match std::env::var(MAPACHE_TEST_VERBOSE) {
    Ok(s) => !(s.parse::<bool>().unwrap_or(false)),
    Err(_) => true,
});
