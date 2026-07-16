#!/usr/bin/env python3
"""
Smoke test for mapache: exercises init, snapshot (full + incremental),
find, verify, restore, diff, stats, forget, and clean against the
Linux kernel source tree (~1.6 GB, ~93 k files).

Requirements: Python 3.8+. The mapache binary must be built before running.
"""

import argparse
import filecmp
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
from pathlib import Path


KERNEL_URL = "https://cdn.kernel.org/pub/linux/kernel/v7.x/linux-7.0.tar.xz"
KERNEL_TAR = "linux-7.0.tar.xz"
KERNEL_DIR = "linux-7.0"

SCRIPT_DIR = Path(__file__).resolve().parent.parent

MAPACHE_ENV = {
    **os.environ,
    "MAPACHE_USERNAME": "smoke",
    "MAPACHE_PASSWORD": "smokepassword",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_mapache(
    mapache: str,
    args: list[str],
    check: bool = True,
    stream: bool = False,
) -> subprocess.CompletedProcess:
    cmd = [str(mapache)] + args
    if stream:
        proc = subprocess.run(cmd, text=True, check=check, env=MAPACHE_ENV)
        return subprocess.CompletedProcess(cmd, proc.returncode, stdout="", stderr="")
    return subprocess.run(
        cmd, capture_output=True, text=True, check=check, env=MAPACHE_ENV
    )


def run_mapache_captured(
    mapache: str, args: list[str]
) -> subprocess.CompletedProcess:
    """Run and always capture output (for retry after stream failure)."""
    cmd = [str(mapache)] + args
    return subprocess.run(cmd, capture_output=True, text=True, env=MAPACHE_ENV)


def count_files(path: Path) -> int:
    return sum(1 for _ in path.rglob("*") if _.is_file())


def count_dirs(path: Path) -> int:
    return sum(1 for _ in path.rglob("*") if _.is_dir())


def dircmp_summary(a: Path, b: Path) -> list[str]:
    diffs = []
    comp = filecmp.dircmp(a, b)
    for name in comp.left_only:
        diffs.append(f"only in original: {a / name}")
    for name in comp.right_only:
        diffs.append(f"only in restored: {b / name}")
    for name in comp.diff_files:
        diffs.append(f"files differ: {name}")
    for name in comp.funny_files:
        diffs.append(f"inaccessible: {name}")
    for sub in comp.subdirs:
        diffs.extend(dircmp_summary(a / sub, b / sub))
    return diffs


def download_kernel(work_dir: Path) -> Path:
    tar_path = work_dir / KERNEL_TAR
    if tar_path.exists():
        print(f"  Using cached {tar_path}")
        return tar_path
    print("  Downloading kernel (~1.6 GB)...")
    urllib.request.urlretrieve(KERNEL_URL, tar_path)
    return tar_path


def extract_kernel(work_dir: Path, tar_path: Path) -> Path:
    source_dir = work_dir / KERNEL_DIR
    if source_dir.exists():
        print(f"  Using extracted {source_dir}")
        return source_dir
    print("  Extracting kernel...")
    with tarfile.open(tar_path, "r:xz") as tar:
        tar.extractall(work_dir)
    return source_dir


def parse_json_output(stdout: str) -> dict | list | None:
    """Parse the first JSON line from mapache stdout."""
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    default_bin = SCRIPT_DIR / "target" / "release" / "mapache"
    parser = argparse.ArgumentParser(description="Smoke test for mapache")
    parser.add_argument(
        "--bin", type=Path, default=default_bin,
        help="Path to mapache binary (default: target/release/mapache)",
    )
    args = parser.parse_args()

    mapache = args.bin
    work_dir = Path(tempfile.gettempdir()) / "mapache_smoke"
    repo_dir = work_dir / "repo"
    restore_dir = work_dir / "restored"

    work_dir.mkdir(parents=True, exist_ok=True)
    source_dir: Path | None = None

    passed = 0
    failed = 0
    errors: list[str] = []

    def run_test(name: str, fn):
        nonlocal passed, failed
        t0 = time.perf_counter()
        try:
            fn()
            elapsed = time.perf_counter() - t0
            print(f"  PASS  {name}  ({elapsed:.1f}s)")
            passed += 1
        except Exception as e:
            elapsed = time.perf_counter() - t0
            print(f"  FAIL  {name}  ({elapsed:.1f}s)")
            print(f"        {e}")
            errors.append(name)
            failed += 1

    def cleanup():
        smoke_file = source_dir / "SMOKE_TEST_FILE.txt" if source_dir else None
        if smoke_file and smoke_file.exists():
            smoke_file.unlink()
        for d in [repo_dir, restore_dir]:
            if d.exists():
                shutil.rmtree(d)

    def stream_cmd(cmd_args: list[str]) -> subprocess.CompletedProcess:
        """Run a command streaming output. On failure, re-run captured and raise."""
        res = run_mapache(mapache, cmd_args, check=False, stream=True)
        if res.returncode != 0:
            cap = run_mapache_captured(mapache, cmd_args)
            stderr = cap.stderr.strip()
            raise RuntimeError(
                f"exit code {res.returncode}"
                + (f": {stderr}" if stderr else "")
            )
        return res

    def captured_json(cmd_args: list[str]) -> dict | list:
        """Run a command and return parsed JSON output."""
        res = run_mapache(mapache, cmd_args, check=False)
        if res.returncode != 0:
            raise RuntimeError(
                f"exit code {res.returncode}: {res.stderr.strip()}"
            )
        data = parse_json_output(res.stdout)
        if data is None:
            raise RuntimeError(
                f"no JSON in stdout: {res.stdout[:200]}"
            )
        return data

    try:
        # ── Setup ─────────────────────────────────────────────────────
        print("\n[setup]")
        tar_path = download_kernel(work_dir)
        source_dir = extract_kernel(work_dir, tar_path)
        file_count = count_files(source_dir)
        print(f"  Source: {file_count} files")

        # ── Tests ─────────────────────────────────────────────────────
        print("\n[tests]")
        r = ["-r", str(repo_dir)]

        # 1. Init
        def test_init():
            cleanup()
            run_mapache(mapache, ["init"] + r)
            if not repo_dir.exists():
                raise RuntimeError("repo dir not created")

        run_test("init", test_init)

        # 2. Snapshot (full)
        snap1_id = None
        snap1_new_files = 0
        snap1_new_dirs = 0

        def test_snapshot_full():
            nonlocal snap1_id, snap1_new_files, snap1_new_dirs
            stream_cmd(["snapshot", str(source_dir), "--tags", "smoke-test"] + r)
            data = captured_json(["log", "--json"] + r)
            snaps = data.get("snapshots", [])
            if len(snaps) < 1:
                raise RuntimeError(f"expected >=1 snapshot, got {len(snaps)}")
            snap1_id = snaps[0]["id"]
            summary = snaps[0].get("snapshot", {}).get("summary", {})
            snap1_new_files = summary.get("new_files", 0)
            snap1_new_dirs = summary.get("new_dirs", 0)

        run_test("snapshot (full)", test_snapshot_full)

        # 3. Log
        def test_log():
            data = captured_json(["log", "--json"] + r)
            snaps = data.get("snapshots", [])
            if len(snaps) < 1:
                raise RuntimeError("no snapshots in log")
            if "id" not in snaps[0]:
                raise RuntimeError("snapshot missing 'id' field")

        run_test("log", test_log)

        # 4. Find
        def test_find():
            data = captured_json(["find", "Kconfig", "--json"] + r)
            entries = data.get("entries", [])
            if len(entries) < 1:
                raise RuntimeError(f"expected >=1 find result, got {len(entries)}")

        run_test("find", test_find)

        # 5. Verify
        def test_verify():
            stream_cmd(["verify", "--read-packs"] + r)

        run_test("verify (1 snapshot)", test_verify)

        # 6. Restore + diff
        def test_restore_diff():
            restore_dir.mkdir(exist_ok=True)
            stream_cmd(
                ["restore", "latest", "--target", str(restore_dir), "--verify"] + r
            )
            restored_kernel = restore_dir / KERNEL_DIR
            if not restored_kernel.exists():
                raise RuntimeError("restored kernel dir not found")
            diffs = dircmp_summary(source_dir, restored_kernel)
            if diffs:
                sample = diffs[:5]
                more = f" ... and {len(diffs) - 5} more" if len(diffs) > 5 else ""
                raise RuntimeError(f"{len(diffs)} diffs: {sample}{more}")

        run_test("restore + diff", test_restore_diff)

        # 7. Incremental snapshot
        snap2_id = None
        pre_files = 0
        pre_dirs = 0
        post_files = 0
        post_dirs = 0

        def test_snapshot_incremental():
            nonlocal snap2_id, pre_files, pre_dirs, post_files, post_dirs
            pre_files = count_files(source_dir)
            pre_dirs = count_dirs(source_dir)
            smoke_file = source_dir / "SMOKE_TEST_FILE.txt"
            smoke_file.write_text("mapache smoke test marker\n")
            post_files = count_files(source_dir)
            post_dirs = count_dirs(source_dir)
            if post_files != pre_files + 1:
                raise RuntimeError(
                    f"Python count: expected {pre_files + 1} files after add, got {post_files}"
                )
            if post_dirs != pre_dirs:
                raise RuntimeError(
                    f"Python count: expected {pre_dirs} dirs after add, got {post_dirs}"
                )
            try:
                stream_cmd(["snapshot", str(source_dir), "--tags", "incremental"] + r)
                data = captured_json(["log", "--json"] + r)
                snaps = data.get("snapshots", [])
                if len(snaps) < 2:
                    raise RuntimeError(f"expected >=2 snapshots, got {len(snaps)}")
                snap2_id = snaps[-1]["id"]
            finally:
                smoke_file.unlink(missing_ok=True)

        run_test("snapshot (incremental)", test_snapshot_incremental)

        # 8. Diff between snapshots
        def test_diff():
            if not snap1_id or not snap2_id:
                raise RuntimeError("missing snapshot IDs from previous steps")
            res = run_mapache(
                mapache, ["diff", snap1_id, snap2_id, "--json"] + r,
                check=False,
            )
            if res.returncode != 0:
                raise RuntimeError(
                    f"exit code {res.returncode}: {res.stderr.strip()}"
                )
            summary = None
            for line in res.stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                if entry.get("msg_type") == "diff_summary":
                    summary = entry
            if summary is None:
                raise RuntimeError("no diff_summary in output")
            new_files = summary.get("new_files", 0)
            changed_files = summary.get("changed_files", 0)
            deleted_files = summary.get("deleted_files", 0)
            unchanged_files = summary.get("unchanged_files", 0)
            new_dirs = summary.get("new_dirs", 0)
            changed_dirs = summary.get("changed_dirs", 0)
            deleted_dirs = summary.get("deleted_dirs", 0)
            unchanged_dirs = summary.get("unchanged_dirs", 0)
            if new_files != 1:
                raise RuntimeError(f"expected 1 new file, got {new_files}")
            if changed_files != 0:
                raise RuntimeError(f"expected 0 changed files, got {changed_files}")
            if deleted_files != 0:
                raise RuntimeError(f"expected 0 deleted files, got {deleted_files}")
            if unchanged_files != snap1_new_files:
                raise RuntimeError(f"expected {snap1_new_files} unchanged files, got {unchanged_files}")
            # Verify arithmetic: unchanged + new + changed + deleted = snap1_total + new
            total_in_diff = unchanged_files + new_files + changed_files + deleted_files
            if total_in_diff != snap1_new_files + new_files:
                raise RuntimeError(
                    f"arithmetic mismatch: {unchanged_files}+{new_files}+{changed_files}+{deleted_files}"
                    f" = {total_in_diff}, expected {snap1_new_files + new_files}"
                )
            if new_dirs != 0:
                raise RuntimeError(f"expected 0 new dirs, got {new_dirs}")
            if changed_dirs != 1:
                raise RuntimeError(f"expected 1 changed dir (root), got {changed_dirs}")
            if deleted_dirs != 0:
                raise RuntimeError(f"expected 0 deleted dirs, got {deleted_dirs}")
            if unchanged_dirs != snap1_new_dirs - 1:
                raise RuntimeError(
                    f"expected {snap1_new_dirs - 1} unchanged dirs, got {unchanged_dirs}"
                )
            # Verify dir arithmetic
            total_dirs_diff = unchanged_dirs + new_dirs + changed_dirs + deleted_dirs
            if total_dirs_diff != snap1_new_dirs + new_dirs:
                raise RuntimeError(
                    f"dir arithmetic mismatch: {unchanged_dirs}+{new_dirs}+{changed_dirs}+{deleted_dirs}"
                    f" = {total_dirs_diff}, expected {snap1_new_dirs + new_dirs}"
                )

        run_test("diff", test_diff)

        # 9. Stats
        def test_stats():
            data = captured_json(["stats", "--json"] + r)
            if not isinstance(data, dict):
                raise RuntimeError(f"expected dict, got {type(data)}")

        run_test("stats", test_stats)

        # 10. Verify (2 snapshots)
        def test_verify_multi():
            stream_cmd(["verify", "--read-packs"] + r)

        run_test("verify (2 snapshots)", test_verify_multi)

        # 11. Forget + clean
        def test_forget_clean():
            stream_cmd(["forget", "--keep-last", "1"] + r)
            run_mapache(mapache, ["clean"] + r, check=False)
            data = captured_json(["log", "--json"] + r)
            snaps = data.get("snapshots", [])
            if len(snaps) != 1:
                raise RuntimeError(
                    f"expected 1 snapshot after forget, got {len(snaps)}"
                )

        run_test("forget + clean", test_forget_clean)

        # 12. Final verify
        def test_verify_final():
            stream_cmd(["verify", "--read-packs"] + r)

        run_test("verify (post-cleanup)", test_verify_final)

    finally:
        cleanup()

    # ── Summary ────────────────────────────────────────────────────────
    print(f"\n{passed} passed, {failed} failed")
    if errors:
        print(f"Failed: {', '.join(errors)}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
