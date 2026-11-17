use std::process::{ExitCode, Termination};

use mapache::{
    commands,
    common::{runtime_max_blocking_threads, runtime_worker_threads},
};

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

fn main() -> MainExitCode {
    // Build the runtime explicitly so Tokio's blocking pool cannot grow to its
    // default maximum of 512 threads. Return the exit code so destructors (e.g.
    // lock handles) run on drop.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("mapache-async")
        .worker_threads(runtime_worker_threads())
        .max_blocking_threads(runtime_max_blocking_threads())
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime");

    MainExitCode(runtime.block_on(commands::parse_and_run()))
}
