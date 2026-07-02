#!/usr/bin/env python3
r"""Build all Mapache cross-compilation targets via Docker.

Each target is compiled in its own container to avoid cargo's registry lock
contention. Target directories are kept under build/ for easy cleanup.

Usage:
  ./tools/docker/build.py                     # default features
  ./tools/docker/build.py tui                 # custom features
  ./tools/docker/build.py --no-image-rebuild  # skip docker build
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

if sys.platform != "win32":
    import pwd  # noqa: PLC0415  # Unix-only


@dataclasses.dataclass
class Target:
    triple: str
    target_dir: str   # short name for CARGO_TARGET_DIR
    artifact: str
    platform: str     # "linux" | "windows" | "darwin" | "android"
    is_exe: bool
    rustflags: str = ""
    features: str | None = None
    no_default_features: bool = False
    tool: str = "build"  # "build" | "zigbuild" | "xwin"


TARGETS: list[Target] = [
    Target("x86_64-unknown-linux-musl",      "x86_64-musl",      "mapache-linux-amd64",   "linux",   False,
           rustflags="-C target-feature=+crt-static -C relocation-model=pie"),
    Target("aarch64-unknown-linux-musl",     "aarch64-musl",     "mapache-linux-arm64",   "linux",   False,
           rustflags="-C target-feature=+crt-static -C relocation-model=pie", tool="zigbuild"),
    Target("aarch64-linux-android",           "android-arm64",    "mapache-android-arm64", "android", False,
           no_default_features=True, features="tui"),
    Target("armv7-unknown-linux-musleabihf",  "armv7-musl",       "mapache-linux-armv7",  "linux",   False,
           rustflags="-C target-feature=+crt-static -C relocation-model=pie", tool="zigbuild"),
    Target("x86_64-pc-windows-msvc",          "windows-x64",      "mapache-windows-amd64","windows", True,
           rustflags="-C target-feature=+crt-static", tool="xwin"),
    Target("x86_64-apple-darwin",             "darwin-x64",       "mapache-darwin-amd64", "darwin",  False,
           rustflags="-C link-args=-Wl,-undefined,dynamic_lookup", tool="zigbuild"),
    Target("aarch64-apple-darwin",            "darwin-aarch64",   "mapache-darwin-arm64", "darwin",  False,
           rustflags="-C link-args=-Wl,-undefined,dynamic_lookup", tool="zigbuild"),
]

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
IMAGE_NAME = "mapache-builder"
BUILD_PATH = PROJECT_ROOT / "build"

docker = shutil.which("docker")
if docker is None:
    print("error: docker not found in PATH", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def archive_fmt(platform: str) -> str:
    return "zip" if platform in ("windows", "darwin") else "tar.xz"


def check_docker_reachable() -> None:
    try:
        subprocess.run(
            [docker, "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        hint = (
            "Try again with sudo, or add your user to the docker group:\n"
            "  sudo usermod -aG docker $USER"
            if sys.platform != "win32" else
            "Make sure Docker Desktop is running and you are using Linux containers."
        )
        print(
            f"error: docker is not running or the current user lacks permissions.\n  {hint}",
            file=sys.stderr,
        )
        sys.exit(1)


def image_exists(name: str) -> bool:
    return (
        subprocess.run(
            [docker, "image", "inspect", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )


def docker_build() -> None:
    print("> docker build …")
    subprocess.run(
        [docker, "build", "-t", IMAGE_NAME, str(SCRIPT_DIR)],
        check=True,
    )


def build_target(t: Target, features: str, release_type: bool = False) -> None:
    """Compile a single target in its own container."""
    target_path = BUILD_PATH / f"target-{t.target_dir}"

    # Build the cargo subcommand (zigbuild / xwin / build)
    if t.tool == "zigbuild":
        cmd = ["cargo", "zigbuild"]
    elif t.tool == "xwin":
        cmd = ["cargo", "xwin", "build"]
    else:
        cmd = ["cargo", "build"]

    cmd += ["--release", "--target", t.triple, "-p", "mapache"]

    if t.no_default_features:
        cmd.append("--no-default-features")
        if t.features:
            cmd += ["--features", t.features]
    elif t.features:
        cmd += ["--features", t.features]
    elif features:
        cmd += ["--features", features]

    # Use Docker-internal path (source is mounted at /mapache)
    cargo_target_dir = f"/mapache/build/target-{t.target_dir}"
    env = {
        "CARGO_TARGET_DIR": cargo_target_dir,
        "CARGO_PROFILE_RELEASE_LTO": "true",
        "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "1",
    }
    if t.platform == "darwin":
        env["PKG_CONFIG_PATH"] = "/opt/macosx-sdks/MacOSX26.1.sdk/usr/lib/pkgconfig"
    if t.rustflags:
        env["RUSTFLAGS"] = t.rustflags
    if release_type:
        env["MAPACHE_RELEASE_TYPE"] = "release"

    label = f"{t.artifact} ({t.triple})"
    print(f"\n===== {label} =====")

    vol_flags = ":z" if sys.platform != "win32" else ""

    # Run as root (no --user), but fix artifact ownership inside the container
    # before exit so the host sees user-owned files — no sudo needed.
    # Cargo registry is cached in a named Docker volume to speed up rebuilds
    # without leaking files onto the host filesystem.
    chown_cmd = "chown -R --reference=/mapache /mapache/build/"
    docker_args = [
        docker, "run", "--rm",
        "-v", f"{PROJECT_ROOT}:/mapache{vol_flags}",
        "-v", "mapache-cargo-registry:/root/.cargo/registry",
        "-w", "/mapache",
    ]
    for k, v in env.items():
        docker_args += ["-e", f"{k}={v}"]
    docker_args += [IMAGE_NAME, "sh", "-c",
                    f"{' '.join(cmd)} && {chown_cmd}"]

    subprocess.run(docker_args, check=True)


def package_artifacts() -> None:
    BUILD_PATH.mkdir(parents=True, exist_ok=True)

    print("\nPackaging artifacts …")
    for t in TARGETS:
        ext = ".exe" if t.is_exe else ""
        binary = BUILD_PATH / f"target-{t.target_dir}" / t.triple / "release" / f"mapache{ext}"

        if not binary.is_file():
            print(f"  skip  {binary}")
            continue

        fmt = archive_fmt(t.platform)
        archive_name = f"{t.artifact}.{fmt}"
        archive_path = BUILD_PATH / archive_name
        staging = BUILD_PATH / t.artifact

        shutil.copy2(str(binary), str(staging))

        if fmt == "zip":
            with zipfile.ZipFile(str(archive_path), "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(str(staging), f"mapache{ext}")
        else:
            with tarfile.open(str(archive_path), "w:xz") as tf:
                tf.add(str(staging), arcname=f"mapache{ext}")

        staging.unlink()
        print(f"  done  {archive_name}")

    for f in BUILD_PATH.rglob("*"):
        if f.is_file():
            try:
                f.chmod(f.stat().st_mode | 0o444)
            except PermissionError:
                pass

    print(f"Done – artifacts in {BUILD_PATH}/")


def transfer_ownership() -> None:
    """Chown build/ back to the real user (Unix only)."""
    if sys.platform == "win32" or not BUILD_PATH.exists():
        return

    sudo_uid = os.environ.get("SUDO_UID")
    if sudo_uid:
        info = pwd.getpwuid(int(sudo_uid))
    else:
        user = os.environ.get("USER")
        if not user:
            return
        info = pwd.getpwnam(user)

    sample = next(BUILD_PATH.rglob("*"), None)
    if sample is None or sample.stat().st_uid == info.pw_uid:
        return

    print("Fixing artifact ownership (sudo required) …")
    subprocess.run(
        ["sudo", "chown", "-R", f"{info.pw_uid}:{info.pw_gid}", str(BUILD_PATH)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


# ---------------------------------------------------------------------------
# cli
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Build all Mapache cross-compilation targets via Docker.\n"
                    "Each target runs in its own container — no shared state.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "features",
        nargs="?",
        default="default",
        help='Cargo feature set (default: "default")',
    )
    p.add_argument(
        "--no-image-rebuild",
        action="store_true",
        help="Skip docker build when the image already exists",
    )
    p.add_argument(
        "--target",
        action="append",
        dest="targets",
        help="Only build targets whose platform or triple matches (may be given multiple times)",
    )
    p.add_argument(
        "--release-type",
        action="store_true",
        help="Set MAPACHE_RELEASE_TYPE=release for release version strings",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    check_docker_reachable()

    if args.no_image_rebuild and image_exists(IMAGE_NAME):
        print(f"Image {IMAGE_NAME!r} exists, skipping build.")
    else:
        docker_build()

    selected = TARGETS
    if args.targets:
        selected = [
            t for t in TARGETS
            if t.platform in args.targets or t.triple in args.targets
        ]
        if not selected:
            print(f"No targets match {args.targets!r}", file=sys.stderr)
            sys.exit(1)

    for t in selected:
        build_target(t, args.features, release_type=args.release_type)

    package_artifacts()

    # Remove intermediate target directories — only keep the archives
    for d in BUILD_PATH.iterdir():
        if d.is_dir() and d.name.startswith("target-"):
            shutil.rmtree(d, ignore_errors=True)

    transfer_ownership()


if __name__ == "__main__":
    main()
