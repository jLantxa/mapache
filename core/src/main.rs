use mapache::commands;

#[tokio::main]
async fn main() {
    // Parse arguments and execute commands.
    // Intercept errors and exit with code 1 on failure.
    if commands::parse_and_run().await.is_err() {
        std::process::exit(1);
    }
}
