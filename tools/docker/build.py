#!/usr/bin/env python3
"""Build all Mapache cross-compilation targets via Docker.

Each target is compiled in its own container to avoid cargo registry lock
contention. Output goes to build/<ref>_<timestamp>/ with bin/ and packed/.

Usage:
  ./tools/docker/build.py --ref v0.6.0
  ./tools/docker/build.py --ref v0.6.0 --no-image-rebuild
  ./tools/docker/build.py --ref v0.6.0 --target linux
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

if sys.platform != "win32":
    import pwd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
TARGETS_FILE = SCRIPT_DIR / "targets"
IMAGE_NAME = "mapache-builder"

docker = shutil.which("docker")
if docker is None:
    print("error: docker not found in PATH", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Target: read from targets file, derive extras from triple
# ---------------------------------------------------------------------------

def _platform_from_triple(triple: str) -> str:
    if "-linux-" in triple:
        return "linux"
    if "-android" in triple:
        return "android"
    if "-windows-" in triple:
        return "windows"
    if "-darwin" in triple:
        return "darwin"
    return "unknown"


def _tool_from_triple(triple: str) -> str:
    if triple == "x86_64-unknown-linux-musl":
        return "build"
    if triple == "x86_64-pc-windows-msvc":
        return "xwin"
    return "zigbuild"


def _target_dir_from_triple(triple: str) -> str:
    """Short name for CARGO_TARGET_DIR."""
    parts = triple.replace("-unknown-", "-").replace("-pc-", "-").split("-")
    return "-".join(parts[:2])


class Target:
    __slots__ = (
        "triple", "release_name", "rustflags", "feat_args",
        "platform", "is_exe", "tool", "target_dir",
    )

    def __init__(self, triple: str, release_name: str, rustflags: str, feat_args: str):
        self.triple = triple
        self.release_name = release_name
        self.rustflags = rustflags
        self.feat_args = feat_args
        self.platform = _platform_from_triple(triple)
        self.is_exe = self.platform == "windows"
        self.tool = _tool_from_triple(triple)
        self.target_dir = _target_dir_from_triple(triple)


def read_targets() -> list[Target]:
    targets: list[Target] = []
    for line in TARGETS_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = shlex.split(line)
        if len(parts) < 4:
            parts += [""] * (4 - len(parts))
        targets.append(Target(*parts))
    return targets


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def check_docker_reachable() -> None:
    try:
        subprocess.run(
            [docker, "info"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("error: docker is not running or lacks permissions", file=sys.stderr)
        sys.exit(1)


def image_exists() -> bool:
    return subprocess.run(
        [docker, "image", "inspect", IMAGE_NAME],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode == 0


def docker_build() -> None:
    print("> docker build …")
    subprocess.run([docker, "build", "-t", IMAGE_NAME, str(SCRIPT_DIR)], check=True)


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

def build_target(t: Target, release_type: bool = False) -> None:
    build_path = PROJECT_ROOT / "build" / f"target-{t.target_dir}"
    cargo_target_dir = f"/mapache/build/target-{t.target_dir}"

    cmd: list[str] = []
    if t.tool == "zigbuild":
        cmd = ["cargo", "zigbuild"]
    elif t.tool == "xwin":
        cmd = ["cargo", "xwin", "build"]
    else:
        cmd = ["cargo", "build"]
    cmd += ["--release", "--target", t.triple, "-p", "mapache"]

    if t.feat_args:
        cmd += t.feat_args.split()

    env = {
        "CARGO_TARGET_DIR": cargo_target_dir,
        "CARGO_PROFILE_RELEASE_LTO": "true",
        "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "1",
    }
    if t.platform == "darwin":
        env["PKG_CONFIG_PATH"] = "/opt/macosx-sdks/MacOSX26.1.sdk/usr/lib/pkgconfig"
    rustflags_parts: list[str] = []
    if t.platform == "darwin":
        rustflags_parts.append("-C link-arg=-Wl,-undefined,dynamic_lookup")
    if t.rustflags:
        rustflags_parts.append(t.rustflags)
    if rustflags_parts:
        env["RUSTFLAGS"] = " ".join(rustflags_parts)
    if release_type:
        env["MAPACHE_RELEASE_TYPE"] = "release"

    label = f"{t.release_name} ({t.triple})"
    print(f"\n===== {label} =====")

    vol_flags = ":z" if sys.platform != "win32" else ""
    chown_cmd = "chown -R --reference=/mapache /mapache/build/"

    docker_args = [
        docker, "run", "--rm",
        "-v", f"{PROJECT_ROOT}:/mapache{vol_flags}",
        "-v", "mapache-cargo-registry:/root/.cargo/registry",
        "-w", "/mapache",
    ]
    for k, v in env.items():
        docker_args += ["-e", f"{k}={v}"]
    docker_args += [IMAGE_NAME, "sh", "-c", f"{' '.join(cmd)} && {chown_cmd}"]

    subprocess.run(docker_args, check=True)


# ---------------------------------------------------------------------------
# pack
# ---------------------------------------------------------------------------

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def pack(ref: str, targets: list[Target]) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out = PROJECT_ROOT / "build" / f"{ref}_{ts}"
    bin_dir = out / "bin"
    packed_dir = out / "packed"
    bin_dir.mkdir(parents=True)
    packed_dir.mkdir(parents=True)

    print(f"\n=== Packing build {ref} ===\n")

    # --- bin/ ---
    checksums_bin: list[str] = []
    for t in targets:
        ext = ".exe" if t.is_exe else ""
        binary = PROJECT_ROOT / "build" / f"target-{t.target_dir}" / t.triple / "release" / f"mapache{ext}"
        if not binary.is_file():
            print(f"  skip  {t.release_name}")
            continue
        full_name = f"mapache_{ref}_{t.release_name}{ext}"
        dest = bin_dir / full_name
        shutil.copy2(str(binary), str(dest))
        h = sha256_file(dest)
        checksums_bin.append(f"{h}  {full_name}")
        print(f"  bin/{full_name}")

    (bin_dir / "checksums.txt").write_text("\n".join(checksums_bin) + "\n")
    print(f"  bin/checksums.txt")

    # --- packed/ ---
    print()
    for t in targets:
        ext = ".exe" if t.is_exe else ""
        full_name = f"mapache_{ref}_{t.release_name}{ext}"
        src = bin_dir / full_name
        if not src.is_file():
            continue

        if t.platform in ("windows", "darwin"):
            archive_name = f"{full_name}.zip"
            archive_path = packed_dir / archive_name
            with zipfile.ZipFile(str(archive_path), "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(str(src), src.name)
        else:
            archive_name = f"{full_name}.tar.xz"
            archive_path = packed_dir / archive_name
            with tarfile.open(str(archive_path), "w:xz") as tf:
                tf.add(str(src), arcname=src.name)

        print(f"  packed/{archive_name}")

    (packed_dir / "checksums.txt").write_text("\n".join(checksums_bin) + "\n")

    print(f"\n=== Build output ===")
    print(f"  {out}/")
    for f in sorted(bin_dir.iterdir()):
        print(f"    bin/{f.name}")
    for f in sorted(packed_dir.iterdir()):
        print(f"    packed/{f.name}")


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------

def cleanup() -> None:
    build_dir = PROJECT_ROOT / "build"
    if not build_dir.exists():
        return
    for d in build_dir.iterdir():
        if d.is_dir() and d.name.startswith("target-"):
            shutil.rmtree(d, ignore_errors=True)


# ---------------------------------------------------------------------------
# ownership
# ---------------------------------------------------------------------------

def transfer_ownership() -> None:
    if sys.platform == "win32":
        return
    build_dir = PROJECT_ROOT / "build"
    if not build_dir.exists():
        return
    sample = next(build_dir.rglob("*"), None)
    if sample is None:
        return
    try:
        user = os.environ.get("USER", "")
        if not user:
            return
        info = pwd.getpwnam(user)
        if sample.stat().st_uid == info.pw_uid:
            return
        subprocess.run(
            ["sudo", "chown", "-R", f"{info.pw_uid}:{info.pw_gid}", str(build_dir)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except (KeyError, PermissionError):
        pass


# ---------------------------------------------------------------------------
# cli
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(
        description="Build all Mapache cross-compilation targets via Docker.",
    )
    p.add_argument("--ref", required=True, help="Version ref (e.g. v0.6.0)")
    p.add_argument("--no-image-rebuild", action="store_true",
                   help="Skip docker build when the image already exists")
    p.add_argument("--target", action="append", dest="targets",
                   help="Only build targets matching platform or triple")
    p.add_argument("--release-type", action="store_true",
                   help="Set MAPACHE_RELEASE_TYPE=release")
    args = p.parse_args()

    check_docker_reachable()

    if args.no_image_rebuild and image_exists():
        print(f"Image {IMAGE_NAME!r} exists, skipping build.")
    else:
        docker_build()

    all_targets = read_targets()
    selected = all_targets
    if args.targets:
        selected = [
            t for t in all_targets
            if t.platform in args.targets or t.triple in args.targets
        ]
        if not selected:
            print(f"No targets match {args.targets!r}", file=sys.stderr)
            sys.exit(1)

    for t in selected:
        build_target(t, release_type=args.release_type)

    pack(args.ref, selected)
    cleanup()
    transfer_ownership()


if __name__ == "__main__":
    main()
