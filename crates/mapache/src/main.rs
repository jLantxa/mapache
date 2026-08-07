use std::process::{ExitCode, Termination};

use mapache::commands;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"narenas:1,tcache:true,dirty_decay_ms:10,muzzy_decay_ms:10\0";

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
