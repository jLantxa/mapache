fn main() {
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=MAPACHE_BUILD_TARGET={target}");

    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let rustc_ver = std::process::Command::new(&rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| {
            s.trim()
                .split('(')
                .next()
                .unwrap_or("unknown")
                .trim()
                .to_string()
        })
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=MAPACHE_RUSTC_VERSION={rustc_ver}");
}
