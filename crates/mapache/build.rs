use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=MAPACHE_BUILD_TARGET={target}");

    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let rustc_ver = Command::new(&rustc)
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

    println!("cargo:rerun-if-env-changed=MAPACHE_BUILD_TYPE");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap_or_default());
    let git_dir = find_git_dir(&manifest_dir);

    // Decide the build type from the build context: a crates.io tarball ships
    // without a .git dir (release), a git checkout at a tagged commit is a
    // release, and any other git state (branch, detached commit) is a
    // development build. CI can force a release build explicitly with
    // MAPACHE_BUILD_TYPE=release.
    let no_git = git_dir.is_none();
    let on_tag = git_dir.as_ref().is_some_and(|_| is_on_tag(&manifest_dir));
    let release = env::var("MAPACHE_BUILD_TYPE").as_deref() == Ok("release") || no_git || on_tag;

    let pkg_version = env::var("CARGO_PKG_VERSION").unwrap_or_default();
    let version = if release {
        format!("v{pkg_version}")
    } else {
        format!("v{pkg_version}+dev")
    };
    println!(
        "cargo:rustc-env=MAPACHE_BUILD_TYPE={}",
        if release { "release" } else { "dev" }
    );
    println!("cargo:rustc-env=MAPACHE_VERSION={version}");
}

fn find_git_dir(start: &Path) -> Option<PathBuf> {
    let mut dir = Some(start.to_path_buf());
    while let Some(d) = dir {
        let candidate = d.join(".git");
        if candidate.exists() {
            return Some(candidate);
        }
        dir = d.parent().map(Path::to_path_buf);
    }
    None
}

fn run_git(args: &[&str], cwd: &Path) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout)
        .ok()
        .map(|s| s.trim().to_string())
}

fn is_on_tag(cwd: &Path) -> bool {
    run_git(&["describe", "--exact-match", "--tags", "HEAD"], cwd).is_some()
}
