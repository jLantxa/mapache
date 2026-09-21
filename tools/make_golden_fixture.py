#!/usr/bin/env python3
"""
Create a golden fixture repository (v1 or v2) for the format-compatibility test.

The fixture is a real mapache repository containing:
  (a) a snapshot of the `crates/` subtree of the given source,
  (b) a snapshot of the whole source tree,
  (c) a snapshot of the whole source tree with a one-line README change.

After the snapshots are created, each snapshot is restored and byte-compared
against the expected content, so the fixture is guaranteed consistent before
it is packaged.

Outputs written to --out:
  golden_<fmt>.tar.gz      the repository, as a gzipped tar archive
  expected_<fmt>.json      per-snapshot file manifest (path -> [size, blake3])
  provenance_<fmt>.json    versions, snapshot IDs, git state, used commands
  golden_<fmt>.tar.gz.sha256

The convention is to drop these into crates/mapache/tests/fixtures/golden_repo/.

The manifest uses BLAKE3 to match mapache's own content IDs; the Python
`blake3` module is required (pip install blake3).

Use the official v0.6.0 binary for --format v1 and a current master build for
--format v2 (which needs --format and --ecc support).

Usage:
  python3 tools/make_golden_fixture.py \
      --source /path/to/mapache-src \
      --mapache /path/to/mapache \
      --format v2 --ecc 20 \
      --out crates/mapache/tests/fixtures/golden_repo
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

try:
    import blake3
except ImportError:
    sys.exit("the Python 'blake3' module is required (pip install blake3)")

EXCLUDES = ["**/.git", "**/target", "**/book"]
EXCLUDED_DIRS = {".git", "target", "book"}
MARKER = "golden-fixture-marker\n"

MAPACHE_ENV = {
    **os.environ,
    "MAPACHE_USERNAME": "golden",
    "MAPACHE_PASSWORD": "golden-password",
}


def run(mapache, args, check=True):
    cmd = [str(mapache)] + [str(a) for a in args]
    print("+", " ".join(cmd))
    proc = subprocess.run(
        cmd, capture_output=True, text=True, check=False, env=MAPACHE_ENV
    )
    if check and proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr, file=sys.stderr)
        if proc.stdout:
            print(proc.stdout, file=sys.stderr)
        proc.check_returncode()
    return proc


def mapache_version(mapache):
    proc = run(mapache, ["--version"], check=False)
    out = (proc.stdout or "").strip() or (proc.stderr or "").strip()
    return out or "unknown"


def command_help(mapache, subcommand):
    proc = run(mapache, [subcommand, "--help"], check=False)
    return (proc.stdout or "") + (proc.stderr or "")


def collect_manifest(root):
    manifest = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).parts
        if any(part in EXCLUDED_DIRS for part in rel):
            continue
        data = path.read_bytes()
        manifest["/".join(rel)] = [
            len(data),
            blake3.blake3(data).hexdigest(),
        ]
    return manifest


def prefix_manifest(manifest, prefix):
    return {f"{prefix}/{path}": value for path, value in manifest.items()}


def has_command(mapache, subcommand):
    proc = run(mapache, [subcommand, "--help"], check=False)
    return proc.returncode == 0


def new_snapshot_id(repo, before):
    after = {
        p.name
        for p in (repo / "snapshots").iterdir()
        if not p.name.endswith(".ecc")
    }
    diff = after - before
    if len(diff) != 1:
        raise SystemExit(f"expected exactly one new snapshot, got {sorted(diff)}")
    return diff.pop()


def git_info(source):
    if not (source / ".git").exists():
        return None
    def git(*args):
        proc = subprocess.run(
            ["git", "-C", str(source), *args],
            capture_output=True, text=True, check=False,
        )
        return proc.stdout.strip()
    status = git("status", "--porcelain")
    return {
        "head": git("rev-parse", "HEAD"),
        "short_head": git("rev-parse", "--short", "HEAD"),
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "describe": git("describe", "--tags", "--always"),
        "worktree_clean": status == "",
    }


def compare_manifests(expected, actual, label):
    exp_paths = set(expected)
    act_paths = set(actual)
    diffs = []
    for p in sorted(exp_paths - act_paths):
        diffs.append(f"{label}: missing in restore: {p}")
    for p in sorted(act_paths - exp_paths):
        diffs.append(f"{label}: unexpected in restore: {p}")
    for p in sorted(exp_paths & act_paths):
        if expected[p] != actual[p]:
            diffs.append(f"{label}: content differs: {p}")
    return diffs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--mapache", required=True, type=Path)
    parser.add_argument("--format", required=True, choices=["v1", "v2"])
    parser.add_argument("--ecc", type=int, default=None)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()

    source = args.source.resolve()
    mapache = args.mapache.resolve()
    if not source.is_dir():
        parser.error(f"source directory not found: {source}")
    if not mapache.is_file():
        parser.error(f"mapache binary not found: {mapache}")

    format_num = int(args.format[1])
    if args.ecc is not None and not (0 <= args.ecc <= 100):
        parser.error("--ecc must be in 0..=100")

    info = {"source": str(source), "mapache": mapache_version(mapache)}
    info["_commands"] = []
    info["_capabilities"] = {}

    has_format = "--format " in command_help(mapache, "init")
    has_ecc = "--ecc " in command_help(mapache, "init")
    info["_capabilities"] = {"init_format": has_format, "init_ecc": has_ecc}

    if format_num == 2 and not has_format:
        sys.exit("v2 fixtures require a master build with --format support")
    if format_num == 2 and args.ecc is not None and not has_ecc:
        sys.exit("the binary does not support --ecc; use a master build")

    info["snapshots"] = {}
    info["marker"] = MARKER
    info["excludes"] = EXCLUDES
    info["git"] = git_info(source)

    crates = source / "crates"
    if not crates.is_dir():
        sys.exit(f"expected a {crates} directory in the source tree")

    with tempfile.TemporaryDirectory(prefix="mapache-golden-") as tmp:
        tmp = Path(tmp)
        repo = tmp / "repo"
        restore_root = tmp / "restore"
        restore_root.mkdir()

        init_args = ["init", "-r", repo]
        if has_format:
            init_args.append("--format")
            init_args.append(str(format_num))
        if args.ecc is not None:
            init_args.append("--ecc")
            init_args.append(str(args.ecc))
        run(mapache, init_args)
        info["_commands"].append("init " + " ".join(map(str, init_args[1:])))

        exclude_args = []
        for pattern in EXCLUDES:
            exclude_args += ["--exclude", pattern]

        expected_a = prefix_manifest(collect_manifest(crates), crates.name)
        expected_b = prefix_manifest(collect_manifest(source), source.name)

        readme = source / "README.md"
        readme_backup = tmp / "README.md.orig"
        shutil.copy2(readme, readme_backup)
        readmedata = readme.read_bytes()
        readme_modified = readmedata + MARKER.encode()
        expected_c = dict(expected_b)
        expected_c[f"{source.name}/README.md"] = [
            len(readme_modified),
            blake3.blake3(readme_modified).hexdigest(),
        ]

        def snapshot_and_id(path, label):
            before = {
                p.name
                for p in (repo / "snapshots").iterdir()
                if not p.name.endswith(".ecc")
            }
            run(mapache, ["snapshot", "-r", repo, path, *exclude_args])
            snap_id = new_snapshot_id(repo, before)
            info["snapshots"][label] = snap_id
            info["_commands"].append(f"snapshot {label} <- {path}")

        snapshot_and_id(crates, "a")
        snapshot_and_id(source, "b")

        readme.write_bytes(readme_modified)
        try:
            snapshot_and_id(source, "c")
        finally:
            shutil.copy2(readme_backup, readme)

        if has_command(mapache, "verify"):
            run(mapache, ["verify", "-r", repo])
            info["_commands"].append("verify")
        else:
            print("WARNING: mapache has no verify subcommand; skipping")

        errors = []
        for label, expected in [("a", expected_a), ("b", expected_b), ("c", expected_c)]:
            target = restore_root / label
            run(mapache, ["restore", "-r", repo, "--target", target, info["snapshots"][label]])
            actual = collect_manifest(target)
            errors.extend(compare_manifests(expected, actual, label))
            info["_commands"].append(f"restore {label}")
        if errors:
            for line in errors:
                print("ERROR:", line)
            sys.exit("restore verification failed; fixture not produced")

        args.out.mkdir(parents=True, exist_ok=True)
        archive = args.out / f"golden_{args.format}.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for path in sorted(repo.rglob("*")):
                tar.add(
                    path,
                    arcname=path.relative_to(repo).as_posix(),
                    recursive=False,
                )

        (args.out / f"golden_{args.format}.tar.gz.sha256").write_text(
            hashlib.sha256(archive.read_bytes()).hexdigest() + "\n"
        )
        (args.out / f"expected_{args.format}.json").write_text(
            json.dumps({"a": expected_a, "b": expected_b, "c": expected_c}, indent=1)
        )
        (args.out / f"provenance_{args.format}.json").write_text(
            json.dumps(info, indent=1)
        )

    print(
        f"Created golden_{args.format}.tar.gz in {args.out}\n"
        f"  snapshots: a={info['snapshots']['a']} "
        f"b={info['snapshots']['b']} c={info['snapshots']['c']}\n"
        f"  mapache: {info['mapache']}"
    )


if __name__ == "__main__":
    main()