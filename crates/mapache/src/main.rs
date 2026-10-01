use std::process::{ExitCode, Termination};

use mapache::commands;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc configuration, read once during allocator init.
///
/// jemalloc is built here with a symbol prefix, so the variable it consults is
/// `_rjem_malloc_conf`; exporting the unprefixed `malloc_conf` is silently
/// ignored. The symbol also has to survive into the dynamic symbol table, which
/// `.cargo/config.toml` arranges. Without both, this string never applies.
///
/// Arena count and background thread behaviour are left at jemalloc's own
/// defaults, which scale themselves to the machine and the load. Only the decay
/// settings are pinned, to trade a little allocator throughput for returning
/// freed memory to the OS promptly rather than letting a long operation sit at
/// its high-water mark.
#[cfg(target_os = "linux")]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static JEMALLOC_CONF: &[u8] = b"dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

struct MainExitCode(i32);

impl Termination for MainExitCode {
    fn report(self) -> ExitCode {
        ExitCode::from(self.0 as u8)
    }
}

#[tokio::main]
async fn main() -> MainExitCode {
    // Parse arguments and execute commands.
    // Return the exit code so destructors (e.g. lock handles) run on drop.
    MainExitCode(commands::parse_and_run().await)
}
