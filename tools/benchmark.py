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

KERNEL_7_0_URL = "https://cdn.kernel.org/pub/linux/kernel/v7.x/linux-7.0.tar.xz"

@dataclass
class Measurement:
    name: str
    action: str
    tool: str
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

def run_bench(name: str, tool: str, cmd: List[str], env: Dict[str, str]) -> Measurement:
    print(f"  Running {tool} {name}...")

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
            # Format is "95%"
            cpu_percent_time = float(line.split(":")[-1].strip().replace("%", ""))

    # Calculate average CPU usage from accumulated times as a secondary check
    avg_cpu = cpu_percent_time
    if monitor.total_cpu_time > 0 and wall_time > 0:
        # total_cpu_time is in seconds. wall_time is in seconds.
        # (total_cpu_time / wall_time) * 100 gives the multi-core percentage
        avg_cpu = (monitor.total_cpu_time / wall_time) * 100

    return Measurement(
        name=name,
        action="",
        tool=tool,
        wall_time=wall_time,
        peak_rss_kb=peak_rss,
        avg_cpu_percent=avg_cpu,
        samples=monitor.samples
    )

def setup(bench_dir: Path):
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

    # Download and extract if not present
    for url, name in [(KERNEL_7_0_URL, "linux-7.0")]:
        tar_path = BENCH_ROOT / f"{name}.tar.xz"
        if not tar_path.exists():
            print(f"  Downloading {name}...")
            subprocess.run(["curl", "-L", url, "-o", str(tar_path)], check=True)

        target_path = SOURCE_DIR / name
        if not target_path.exists():
            print(f"  Extracting {name} to {target_path}...")
            subprocess.run(["tar", "-xf", str(tar_path), "-C", str(SOURCE_DIR)], check=True)

def cleanup_restores():
    if RESTORE_DIR.exists():
        shutil.rmtree(RESTORE_DIR)
    RESTORE_DIR.mkdir(exist_ok=True)

def main():
    parser = argparse.ArgumentParser(description="Benchmark mapache vs restic")
    parser.add_argument("--mapache", default="mapache", help="Path to mapache binary")
    parser.add_argument("--restic", default="restic", help="Path to restic binary")
    parser.add_argument("--iterations", type=int, default=1, help="Number of times to run each benchmark")
    parser.add_argument("--bench-dir", default="/tmp/mapache_bench", help="Directory for benchmark data")
    args = parser.parse_args()

    setup(Path(args.bench_dir))

    results = []

    # Environment variables
    mapache_env = os.environ.copy()
    mapache_env.update({"MAPACHE_USERNAME": "bench", "MAPACHE_PASSWORD": "benchpassword"})
    restic_env = os.environ.copy()
    restic_env.update({"RESTIC_PASSWORD": "benchpassword"})

    for i in range(args.iterations + 1):
        is_warmup = i == 0
        if is_warmup:
            print(f"\nWarmup Iteration (Results will be discarded)...")
        else:
            print(f"\nIteration {i}/{args.iterations}")

        # --- MAPACHE ---
        print("\nBenchmarking mapache...")
        if REPO_DIR_MAPACHE.exists(): 
            print(f"  Cleaning up existing mapache repository at {REPO_DIR_MAPACHE}...")
            shutil.rmtree(REPO_DIR_MAPACHE)

        print(f"  Initializing fresh mapache repository...")
        subprocess.run([args.mapache, "init", "-r", str(REPO_DIR_MAPACHE)], env=mapache_env, check=True, capture_output=True)

        # Backup
        m = run_bench(f"backup_{'warmup' if is_warmup else i}", "mapache", 
                      [args.mapache, "snapshot", str(SOURCE_DIR / "linux-7.0"), "-r", str(REPO_DIR_MAPACHE), "--json", "--readers", "4"], 
                      mapache_env)
        m.action = "backup"
        m.repo_size_bytes = get_dir_size(REPO_DIR_MAPACHE)
        if not is_warmup:
            results.append(m)

        # Restore
        cleanup_restores()
        m = run_bench(f"restore_{'warmup' if is_warmup else i}", "mapache", 
                      [args.mapache, "restore", "-r", str(REPO_DIR_MAPACHE), "--target", str(RESTORE_DIR), "latest"], 
                      mapache_env)
        m.action = "restore"
        if not is_warmup:
            results.append(m)

        # --- RESTIC ---
        print("\nBenchmarking restic...")
        if REPO_DIR_RESTIC.exists(): 
            print(f"  Cleaning up existing restic repository at {REPO_DIR_RESTIC}...")
            shutil.rmtree(REPO_DIR_RESTIC)

        print(f"  Initializing fresh restic repository...")
        subprocess.run([args.restic, "init", "-r", str(REPO_DIR_RESTIC)], env=restic_env, check=True, capture_output=True)

        # Backup
        m = run_bench(f"backup_{'warmup' if is_warmup else i}", "restic", 
                      [args.restic, "backup", str(SOURCE_DIR / "linux-7.0"), "-r", str(REPO_DIR_RESTIC), "--json", "--read-concurrency", "4"], 
                      restic_env)
        m.action = "backup"
        m.repo_size_bytes = get_dir_size(REPO_DIR_RESTIC)
        if not is_warmup:
            results.append(m)

        # Restore
        cleanup_restores()
        m = run_bench(f"restore_{'warmup' if is_warmup else i}", "restic", 
                      [args.restic, "restore", "latest", "-r", str(REPO_DIR_RESTIC), "--target", str(RESTORE_DIR)], 
                      restic_env)
        m.action = "restore"
        if not is_warmup:
            results.append(m)
    # Save results
    results_file = BENCH_ROOT / "results.json"
    with open(results_file, "w") as f:
        json.dump([asdict(r) for r in results], f, indent=2)

    print(f"\nResults saved to: {results_file}")

    # Aggregate results
    aggregated = {}
    for r in results:
        key = (r.tool, r.action)
        if key not in aggregated:
            aggregated[key] = {
                "times": [],
                "peak_rss": [],
                "cpus": [],
                "repo_sizes": []
            }
        aggregated[key]["times"].append(r.wall_time)
        aggregated[key]["peak_rss"].append(r.peak_rss_kb)
        aggregated[key]["cpus"].append(r.avg_cpu_percent)
        if r.repo_size_bytes > 0:
            aggregated[key]["repo_sizes"].append(r.repo_size_bytes)

    # Print summary table
    print("\n" + "="*125)
    print(f"{'Tool':<10} | {'Action':<10} | {'Avg Time (s)':<15} | {'Max Time (s)':<15} | {'Avg RSS (MB)':<15} | {'Peak RSS (MB)':<15} | {'Avg CPU (%)':<12} | {'Repo (MB)':<10}")
    print("-" * 125)
    for (tool, action), data in sorted(aggregated.items()):
        avg_time = sum(data["times"]) / len(data["times"])
        max_time = max(data["times"])
        avg_rss = (sum(data["peak_rss"]) / len(data["peak_rss"])) / 1024
        peak_rss = max(data["peak_rss"]) / 1024
        avg_cpu = sum(data["cpus"]) / len(data["cpus"])
        repo_size = (sum(data["repo_sizes"]) / len(data["repo_sizes"])) / (1024*1024) if data["repo_sizes"] else 0.0

        print(f"{tool:<10} | {action:<10} | {avg_time:>15.2f} | {max_time:>15.2f} | {avg_rss:>15.2f} | {peak_rss:>15.2f} | {avg_cpu:>12.2f} | {repo_size:>10.2f}")
    print("="*125)

if __name__ == "__main__":
    main()
