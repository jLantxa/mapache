use mapache::commands;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"narenas:1,tcache:true,dirty_decay_ms:10,muzzy_decay_ms:10\0";

#[tokio::main]
async fn main() {
    // Parse arguments and execute commands.
    // Intercept errors and exit with code 1 on failure.
    if commands::parse_and_run().await.is_err() {
        std::process::exit(1);
    }
}
