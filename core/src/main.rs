use mapache::commands;

fn main() {
    // Parse arguments and execute commands.
    // Intercept errors and exit with code 1 on failure.
    if commands::parse_and_run().is_err() {
        std::process::exit(1);
    }
}
