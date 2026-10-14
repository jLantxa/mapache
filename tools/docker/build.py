#!/usr/bin/env python3
"""Build all Mapache cross-compilation targets via Docker.

Each target is compiled in its own container to avoid cargo registry lock
contention. Output goes to build/<ref>_<timestamp>/ with bin/ and packed/.

With --parallel N, each target's container output is written to
build/<ref>_<timestamp>/logs/<release_name>.log and the terminal only shows
one status line per target.

Usage:
  ./tools/docker/build.py --ref v0.6.0
  ./tools/docker/build.py --ref v0.6.0 --no-image-rebuild
  ./tools/docker/build.py --ref v0.6.0 --target linux
  ./tools/docker/build.py --ref v0.6.0 --parallel 4
  ./tools/docker/build.py --ref v0.6.0 --dry-run
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
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
TARGETS_FILE = SCRIPT_DIR / "targets"
IMAGE_NAME = "mapache-builder"

# Must match the CARGO_HOME / registry mount inside the image (see Dockerfile).
CARGO_HOME = "/usr/local/cargo"
REGISTRY_VOLUME = "mapache-cargo-registry"

# The build runs as the host user so artifacts are never owned by root.
if sys.platform != "win32":
    HOST_UID: int | None = os.getuid()
    HOST_GID: int | None = os.getgid()
else:
    HOST_UID = HOST_GID = None

docker = shutil.which("docker")
if docker is None:
    print("error: docker not found in PATH", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Target: read from targets file, derive extras from triple
# ---------------------------------------------------------------------------

def _platform_from_triple(triple: str) -> str:
    # NOTE: match "-android" before "-linux-" because Android triples such as
    # aarch64-linux-android also contain "-linux-".
    if "-android" in triple:
        return "android"
    if "-linux-" in triple:
        return "linux"
    if "-windows-" in triple:
        return "windows"
    if "-darwin" in triple:
        return "darwin"
    return "unknown"


def _tool_from_triple(triple: str) -> str:
    # Android uses the NDK's clang directly (see CC_*/AR_*/LINKER in the
    # Dockerfile), so it builds with plain `cargo build`, not zigbuild.
    if "-android" in triple:
        return "build"
    # The host-musl target is built with the system musl-gcc toolchain.
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

    def __init__(self, triple: str, release_name: str, rustflags: str,
                 feat_args: str, tool: str = ""):
        self.triple = triple
        self.release_name = release_name
        self.rustflags = rustflags
        self.feat_args = feat_args
        self.platform = _platform_from_triple(triple)
        self.is_exe = self.platform == "windows"
        # The targets file is authoritative; fall back to deriving the tool
        # from the triple for older/partial targets files.
        self.tool = tool or _tool_from_triple(triple)
        self.target_dir = _target_dir_from_triple(triple)


def read_targets() -> list[Target]:
    targets: list[Target] = []
    for line in TARGETS_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = shlex.split(line)
        if len(parts) < 5:
            parts += [""] * (5 - len(parts))
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


def prepare_workspace() -> None:
    """Repair ownership of persistent mounts once, before the builds start.

    Older images (and interrupted runs) could leave the cargo registry volume
    or build/ root-owned. Fixing that in a single throwaway root container
    avoids every parallel build container racing to do the same thing.
    """
    if HOST_UID is None or HOST_GID is None:
        return
    vol_flags = ":z" if sys.platform != "win32" else ""
    # The registry only ever has one owner, so a top-level check is enough and
    # keeps repeat runs cheap. build/ can be mixed (interrupted runs), so fix
    # it recursively.
    script = f"""
reg={CARGO_HOME}/registry
if [ -d "$reg" ] && [ "$(stat -c %u "$reg" 2>/dev/null)" != "{HOST_UID}" ]; then
  chown -R {HOST_UID}:{HOST_GID} "$reg"
fi
chown -R {HOST_UID}:{HOST_GID} /mapache/build 2>/dev/null || true
"""
    subprocess.run([
        docker, "run", "--rm", "-u", "0",
        "-v", f"{PROJECT_ROOT}:/mapache{vol_flags}",
        "-v", f"{REGISTRY_VOLUME}:{CARGO_HOME}/registry",
        "--entrypoint", "sh", IMAGE_NAME, "-c", script,
    ], check=True)


# The console is shared by all worker threads when building in parallel, so
# every line is written under a lock to avoid interleaving.
_PRINT_LOCK = threading.Lock()


def _log(msg: str = "") -> None:
    with _PRINT_LOCK:
        print(msg, flush=True)


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _tail(path: Path, n: int) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(lines[-n:])


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

def _target_rustflags(t: Target) -> str:
    """Rust flags for a target.

    On top of the flags declared in the targets file, macOS cross builds use
    dynamic lookup for macFUSE, whose dylib only exists on the host.
    """
    parts: list[str] = []
    if t.platform == "darwin":
        parts.append("-C link-arg=-Wl,-undefined,dynamic_lookup")
    if t.rustflags:
        parts.append(t.rustflags)
    return " ".join(parts)


def build_target(t: Target, release_type: bool = False, dry_run: bool = False,
                 log_file: Path | None = None) -> None:
    cargo_target_dir = f"/mapache/build/target-{t.target_dir}"

    env = {
        "CARGO_TARGET_DIR": cargo_target_dir,
        "CARGO_PROFILE_RELEASE_LTO": "true",
        "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "1",
    }
    if release_type:
        env["MAPACHE_BUILD_TYPE"] = "release"
    if HOST_UID is not None and HOST_GID is not None:
        env["BUILD_UID"] = str(HOST_UID)
        env["BUILD_GID"] = str(HOST_GID)

    vol_flags = ":z" if sys.platform != "win32" else ""

    # Build through the same entry point used by the CI workflow so both
    # workflows compile with identical flags (see tools/docker/build-target.sh).
    build_cmd = " ".join(
        shlex.quote(arg)
        for arg in ["sh", "/build-target.sh", t.triple, _target_rustflags(t), t.feat_args, t.tool]
    )
    if t.platform == "darwin":
        build_cmd = 'export PKG_CONFIG_PATH="$SDKROOT/usr/lib/pkgconfig"; ' + build_cmd

    docker_args = [
        docker, "run", "--rm",
        "-v", f"{PROJECT_ROOT}:/mapache{vol_flags}",
        "-v", f"{REGISTRY_VOLUME}:{CARGO_HOME}/registry",
        "-w", "/mapache",
    ]
    for k, v in env.items():
        docker_args += ["-e", f"{k}={v}"]
    docker_args += [IMAGE_NAME, "sh", "-c", build_cmd]

    if dry_run:
        _log(" ".join(shlex.quote(arg) for arg in docker_args))
        return

    # Without a log file (sequential mode) stream the container output live.
    if log_file is None:
        _log(f"\n===== {t.release_name} ({t.triple}) =====")
        subprocess.run(docker_args, check=True)
        return

    # Parallel mode: capture the container output to a per-target file so the
    # concurrent builds don't interleave on the terminal.
    log_file.parent.mkdir(parents=True, exist_ok=True)
    _log(f"[{t.release_name}] building…")
    start = time.monotonic()
    with open(log_file, "w", encoding="utf-8", errors="replace") as fh:
        proc = subprocess.run(docker_args, stdout=fh, stderr=subprocess.STDOUT)
    duration = time.monotonic() - start

    if proc.returncode != 0:
        _log(f"[{t.release_name}] FAILED after {duration:.0f}s — log: {_rel(log_file)}")
        raise subprocess.CalledProcessError(proc.returncode, docker_args)

    _log(f"[{t.release_name}] ok in {duration:.0f}s — log: {_rel(log_file)}")


# ---------------------------------------------------------------------------
# pack
# ---------------------------------------------------------------------------

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def pack(ref: str, targets: list[Target], out: Path) -> None:
    bin_dir = out / "bin"
    packed_dir = out / "packed"
    bin_dir.mkdir(parents=True, exist_ok=True)
    packed_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== Packing build {ref} ===\n")

    checksums_bin: list[str] = []

    for t in targets:
        base = f"mapache_{ref}_{t.release_name}"
        bin_ext = ".exe" if t.is_exe else ""
        bin_name = f"{base}{bin_ext}"

        binary = PROJECT_ROOT / "build" / f"target-{t.target_dir}" / t.triple / "release" / f"mapache{bin_ext}"
        if not binary.is_file():
            print(f"  skip  {t.release_name}")
            continue

        # copy to bin/
        dest = bin_dir / bin_name
        shutil.copy2(str(binary), str(dest))
        h = sha256_file(dest)
        checksums_bin.append(f"{h}  {bin_name}")
        print(f"  bin/{bin_name}")

        # pack into archive
        arc_ext = ".zip" if t.platform in ("windows", "darwin") else ".tar.xz"
        archive_name = f"{base}{arc_ext}"
        archive_path = packed_dir / archive_name

        if t.platform in ("windows", "darwin"):
            with zipfile.ZipFile(str(archive_path), "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(str(dest), dest.name)
        else:
            with tarfile.open(str(archive_path), "w:xz") as tf:
                tf.add(str(dest), arcname=dest.name)

        print(f"  packed/{archive_name}")

    # Sort by file name for deterministic output (matches the CI workflow).
    checksums_bin.sort(key=lambda line: line.split("  ", 1)[-1])

    (bin_dir / "checksums.txt").write_text("\n".join(checksums_bin) + "\n")
    print(f"  bin/checksums.txt")

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
                   help="Set MAPACHE_BUILD_TYPE=release")
    p.add_argument("--parallel", type=int, metavar="N",
                   help="Build N targets in parallel (default: sequential); "
                        "each target logs to build/<ref>_<ts>/logs/")
    p.add_argument("--dry-run", action="store_true",
                   help="Print docker commands without executing")
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

    if args.dry_run:
        if args.parallel and args.parallel > 1:
            with ThreadPoolExecutor(max_workers=args.parallel) as ex:
                futures = [ex.submit(build_target, t, args.release_type, True) for t in selected]
                for fut in as_completed(futures):
                    fut.result()
        else:
            for t in selected:
                build_target(t, release_type=args.release_type, dry_run=True)
        return

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out = PROJECT_ROOT / "build" / f"{args.ref}_{ts}"

    prepare_workspace()

    if args.parallel and args.parallel > 1:
        # Each build logs to its own file; the terminal only gets one concise
        # line per target so concurrent output stays readable.
        log_dir = out / "logs"
        failures: list[Target] = []
        with ThreadPoolExecutor(max_workers=args.parallel) as ex:
            futures = {
                ex.submit(
                    build_target, t, args.release_type, False,
                    log_dir / f"{t.release_name}.log",
                ): t
                for t in selected
            }
            for fut in as_completed(futures):
                target = futures[fut]
                try:
                    fut.result()
                except subprocess.CalledProcessError:
                    failures.append(target)
        if failures:
            # Print the failure details in one grouped block once every build
            # has finished, so they don't interleave with live status lines.
            print(f"\n=== {len(failures)} target(s) failed ===", file=sys.stderr)
            for t in failures:
                log_file = log_dir / f"{t.release_name}.log"
                tail = _tail(log_file, 40)
                print(f"\n----- {t.release_name} ({t.triple}) — {_rel(log_file)} -----",
                      file=sys.stderr)
                if tail:
                    print(tail, file=sys.stderr)
            names = ", ".join(t.release_name for t in failures)
            print(f"\nerror: {len(failures)} target(s) failed: {names}", file=sys.stderr)
            sys.exit(1)
    else:
        for t in selected:
            build_target(t, release_type=args.release_type)

    pack(args.ref, selected, out)
    cleanup()


if __name__ == "__main__":
    main()
