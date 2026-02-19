use mapache::commands;

#[cfg(all(not(target_env = "msvc"), target_os = "linux"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    // Parse arguments and execute commands.
    // Intercept errors and exit with code 1 on failure.
    if commands::parse_and_run().await.is_err() {
        std::process::exit(1);
    }
}
