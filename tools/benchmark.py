#!/usr/bin/env python3

import os
import sys
import subprocess
import time
import json
import psutil
import shutil
import argparse
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict, Any

# Global paths (will be initialized in main)
BENCH_ROOT = None
SOURCE_DIR = None
REPO_DIR_MAPACHE = None
REPO_DIR_RESTIC = None
RESTORE_DIR = None
LOGS_DIR = None

# Workloads
WORKLOADS = {
    "kernel": {
        "url": "https://cdn.kernel.org/pub/linux/kernel/v7.x/linux-7.0.tar.xz",
        "name": "linux-7.0.tar.xz",
        "extract_name": "linux-7.0"
    },
    "enron": {
        "url": "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz",
        "name": "enron_mail_20150507.tar.gz",
        "extract_name": "maildir"
    }
}

@dataclass
class Measurement:
    name: str
    action: str
    tool: str
    workload: str
    wall_time: float
    peak_rss_kb: int
    avg_cpu_percent: float
    repo_size_bytes: int = 0
    samples: List[Dict[str, Any]] = None

def get_dir_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for entry in os.scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += get_dir_size(Path(entry.path))
    return total

class Monitor:
    def __init__(self, pid, interval=0.2):
        self.pid = pid
        self.interval = interval
        self.samples = []
        self.keep_running = True
        self.total_cpu_time = 0.0
        try:
            self.process = psutil.Process(pid)
        except psutil.NoSuchProcess:
            self.process = None

    def run(self):
        if not self.process:
            return

        while self.keep_running:
            try:
                with self.process.oneshot():
                    # Get RSS for process and all children
                    children = self.process.children(recursive=True)
                    rss = self.process.memory_info().rss

                    # Accumulate CPU time (user + system)
                    cpu_t = self.process.cpu_times()
                    total_t = cpu_t.user + cpu_t.system

                    for child in children:
                        try:
                            rss += child.memory_info().rss
                            c_cpu_t = child.cpu_times()
                            total_t += c_cpu_t.user + c_cpu_t.system
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            continue

                    self.total_cpu_time = total_t
                    self.samples.append({
                        "time": time.time(),
                        "rss_kb": rss // 1024,
                        "cpu_time": total_t
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            time.sleep(self.interval)

def run_bench(name: str, tool: str, workload: str, cmd: List[str], env: Dict[str, str]) -> Measurement:
    print(f"  Running {tool} {name} (Workload: {workload})...", end="")

    # Use /usr/bin/time -v for peak RSS verification
    time_cmd = ["/usr/bin/time", "-v"] + cmd

    start_time = time.time()
    proc = subprocess.Popen(time_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    monitor = Monitor(proc.pid)
    import threading
    t = threading.Thread(target=monitor.run)
    t.start()

    stdout, stderr = proc.communicate()
    end_time = time.time()

    monitor.keep_running = False
    t.join()

    wall_time = end_time - start_time

    # Parse /usr/bin/time -v output
    peak_rss = 0
    cpu_percent_time = 0.0
    for line in stderr.splitlines():
        if "Maximum resident set size (kbytes):" in line:
            peak_rss = int(line.split(":")[-1].strip())
        elif "Percent of CPU this job got:" in line:
            cpu_percent_time = float(line.split(":")[-1].strip().replace("%", ""))

    avg_cpu = cpu_percent_time
    if monitor.total_cpu_time > 0 and wall_time > 0:
        avg_cpu = (monitor.total_cpu_time / wall_time) * 100

    return Measurement(
        name=name,
        action="",
        tool=tool,
        workload=workload,
        wall_time=wall_time,
        peak_rss_kb=peak_rss,
        avg_cpu_percent=avg_cpu,
        samples=monitor.samples
    )

def setup(bench_dir: Path, selected_workloads: List[str]):
    global BENCH_ROOT, SOURCE_DIR, REPO_DIR_MAPACHE, REPO_DIR_RESTIC, RESTORE_DIR, LOGS_DIR
    BENCH_ROOT = bench_dir
    SOURCE_DIR = BENCH_ROOT / "source"
    REPO_DIR_MAPACHE = BENCH_ROOT / "repo_mapache"
    REPO_DIR_RESTIC = BENCH_ROOT / "repo_restic"
    RESTORE_DIR = BENCH_ROOT / "restore"
    LOGS_DIR = BENCH_ROOT / "logs"

    print(f"Setting up benchmark environment in {BENCH_ROOT}...")
    BENCH_ROOT.mkdir(parents=True, exist_ok=True)
    SOURCE_DIR.mkdir(exist_ok=True)
    LOGS_DIR.mkdir(exist_ok=True)

    for w_key in selected_workloads:
        w = WORKLOADS[w_key]
        tar_path = BENCH_ROOT / w["name"]
        if not tar_path.exists():
            print(f"  Downloading {w_key} workload...")
            subprocess.run(["curl", "-L", w["url"], "-o", str(tar_path)], check=True)

        target_path = SOURCE_DIR / w["extract_name"]
        if not target_path.exists():
            print(f"  Extracting {w_key} to {target_path}...")
            # Detect format by extension
            if w["name"].endswith(".xz"):
                subprocess.run(["tar", "-xf", str(tar_path), "-C", str(SOURCE_DIR)], check=True)
            else:
                subprocess.run(["tar", "-zxf", str(tar_path), "-C", str(SOURCE_DIR)], check=True)

def cleanup_restores():
    if RESTORE_DIR.exists():
        shutil.rmtree(RESTORE_DIR)
    RESTORE_DIR.mkdir(exist_ok=True)

def main():
    parser = argparse.ArgumentParser(description="Benchmark mapache vs restic")
    parser.add_argument("--mapache", default="mapache", help="Path to mapache binary")
    parser.add_argument("--restic", default="restic", help="Path to restic binary")
    parser.add_argument("--iterations", type=int, default=1, help="Number of iterations")
    parser.add_argument("--bench-dir", default="/tmp/mapache_bench", help="Directory for benchmark data")
    parser.add_argument("--workloads", default="kernel,enron", help="Workloads (kernel,enron)")
    args = parser.parse_args()

    selected_workloads = [s.strip() for s in args.workloads.split(",")]
    setup(Path(args.bench_dir), selected_workloads)

    results = []
    mapache_env = {**os.environ, "MAPACHE_USERNAME": "bench", "MAPACHE_PASSWORD": "benchpassword"}
    restic_env = {**os.environ, "RESTIC_PASSWORD": "benchpassword"}

    for w_key in selected_workloads:
        w = WORKLOADS[w_key]
        source_path = SOURCE_DIR / w["extract_name"]
        print(f"\n{'#'*40}\n### WORKLOAD: {w_key.upper()} ###\n{'#'*40}")

        # --- MAPACHE BATCH ---
        print(f"\n>>> Starting Mapache sequence...")
        for i in range(args.iterations + 1):
            is_warmup = i == 0
            tag = "warmup" if is_warmup else str(i)
            print(f"  Iteration {tag}...")

            if REPO_DIR_MAPACHE.exists(): shutil.rmtree(REPO_DIR_MAPACHE)
            subprocess.run([args.mapache, "init", "-r", str(REPO_DIR_MAPACHE)], env=mapache_env, check=True, capture_output=True)

            # Backup
            m = run_bench(f"snapshot_{tag}", "mapache", w_key,
                          [args.mapache, "snapshot", str(source_path), "-r", str(REPO_DIR_MAPACHE), "--quiet", "--readers", "8"],
                          mapache_env)
            m.action, m.repo_size_bytes = "backup", get_dir_size(REPO_DIR_MAPACHE)
            if not is_warmup: results.append(m)
            print(f" {m.wall_time:.2f} s")

            # Restore
            cleanup_restores()
            m = run_bench(f"restore_{tag}", "mapache", w_key,
                          [args.mapache, "restore", "--quiet", "-r", str(REPO_DIR_MAPACHE), "--target", str(RESTORE_DIR), "latest"],
                          mapache_env)
            m.action = "restore"
            if not is_warmup: results.append(m)
            print(f" {m.wall_time:.2f} s")

        # --- RESTIC BATCH ---
        print(f"\n>>> Starting Restic sequence...")
        for i in range(args.iterations + 1):
            is_warmup = i == 0
            tag = "warmup" if is_warmup else str(i)
            print(f"  Iteration {tag}...")

            if REPO_DIR_RESTIC.exists(): shutil.rmtree(REPO_DIR_RESTIC)
            subprocess.run([args.restic, "init", "-r", str(REPO_DIR_RESTIC)], env=restic_env, check=True, capture_output=True)

            # Backup
            m = run_bench(f"backup_{tag}", "restic", w_key,
                          [args.restic, "backup", str(source_path), "-r", str(REPO_DIR_RESTIC), "--quiet", "--read-concurrency", "8"],
                          restic_env)
            m.action, m.repo_size_bytes = "backup", get_dir_size(REPO_DIR_RESTIC)
            if not is_warmup: results.append(m)
            print(f" {m.wall_time:.2f} s")

            # Restore
            cleanup_restores()
            m = run_bench(f"restore_{tag}", "restic", w_key,
                          [args.restic, "restore", "--quiet", "latest", "-r", str(REPO_DIR_RESTIC), "--target", str(RESTORE_DIR)],
                          restic_env)
            m.action = "restore"
            if not is_warmup: results.append(m)
            print(f" {m.wall_time:.2f} s")

            print()

    # --- SAVE AND SUMMARY ---
    # (Rest of the aggregation and printing logic remains exactly the same)
    save_and_print_summary(results, Path(args.bench_dir))

def save_and_print_summary(results, bench_root):
    results_file = bench_root / "results.json"

    with open(results_file, "w") as f:
        json.dump([asdict(r) for r in results], f, indent=2)

    aggregated = {}
    for r in results:
        key = (r.workload, r.tool, r.action)
        if key not in aggregated:
            aggregated[key] = { "times": [], "peak_rss": [], "cpus": [], "repo_sizes": [] }
        aggregated[key]["times"].append(r.wall_time)
        aggregated[key]["peak_rss"].append(r.peak_rss_kb)
        aggregated[key]["cpus"].append(r.avg_cpu_percent)
        if r.repo_size_bytes > 0: aggregated[key]["repo_sizes"].append(r.repo_size_bytes)

    print("\n" + "="*145)
    print(f"{'Workload':<10} | {'Tool':<10} | {'Action':<10} | {'Avg Time (s)':<15} | {'Max Time (s)':<15} | {'Avg PSS (MB)':<15} | {'Peak PSS (MB)':<15} | {'Avg CPU (%)':<12} | {'Repo (MB)':<10}")
    print("-" * 145)
    for (workload, tool, action), data in sorted(aggregated.items()):
        avg_t, max_t = sum(data["times"])/len(data["times"]), max(data["times"])
        avg_p, peak_p = (sum(data["peak_rss"])/len(data["peak_rss"]))/1024, max(data["peak_rss"])/1024
        avg_c = sum(data["cpus"])/len(data["cpus"])
        repo = (sum(data["repo_sizes"])/len(data["repo_sizes"]))/(1024**2) if data["repo_sizes"] else 0.0
        print(f"{workload:<10} | {tool:<10} | {action:<10} | {avg_t:>15.2f} | {max_t:>15.2f} | {avg_p:>15.2f} | {peak_p:>15.2f} | {avg_c:>12.2f} | {repo:>10.2f}")

if __name__ == "__main__":
    main()
