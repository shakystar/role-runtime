#!/usr/bin/env python3
"""Sequential real-pi benchmark. Standard library only; no model/network access.

ps reads PID/PPID/RSS/cumulative CPU time only. Only the benchmark process tree
is retained. RSS sums can double-count shared pages and sampling misses peaks.
CPU is a sampled lower-bound, especially for the short-lived fresh baseline.
"""
import argparse
import json
import os
import platform
from pathlib import Path
import random
import signal
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def cpu_seconds(value):
    days = 0
    if "-" in value:
        day, value = value.split("-", 1)
        days = int(day)
    parts = [float(p) for p in value.split(":")]
    if not 1 <= len(parts) <= 3:
        raise ValueError("invalid ps CPU time")
    return days * 86400 + sum(p * 60 ** i for i, p in enumerate(reversed(parts)))


def tree_snapshot(root_pid):
    output = subprocess.check_output(
        ["ps", "-axo", "pid=,ppid=,rss=,time="], text=True, timeout=10
    )
    rows = {}
    for line in output.splitlines():
        pid, parent, rss, cpu = line.split()
        rows[int(pid)] = (int(parent), int(rss), cpu_seconds(cpu))
    selected = {root_pid} if root_pid in rows else set()
    while True:
        extra = {pid for pid, row in rows.items() if row[0] in selected} - selected
        if not extra:
            break
        selected.update(extra)
    return {pid: rows[pid] for pid in selected}


def hardware():
    if platform.system() == "Darwin":
        def read(name):
            return int(subprocess.check_output(["sysctl", "-n", name], text=True))
        return {"memoryBytes": read("hw.memsize"), "logicalCPUs": read("hw.logicalcpu"),
                "physicalCPUs": read("hw.physicalcpu")}
    if platform.system() == "Linux":
        return {"memoryBytes": os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"),
                "logicalCPUs": os.cpu_count(), "physicalCPUs": None}
    raise RuntimeError("resource sampler supports macOS/Linux only")


def run_case(case, interval, max_seconds, rss_limit_mib):
    cmd = [str(ROOT / "bin/role-runtime"), "demo", "--repo", str(ROOT),
           "--roles", str(case["roles"]), "--workers", str(case["workers"]),
           "--rounds", str(case["rounds"]), "--queue", "32",
           "--payload-bytes", str(case.get("payloadBytes", 0)),
           "--tool-delay-ms", str(case.get("toolDelayMS", 15))]
    if case.get("fresh"):
        cmd.append("--fresh")
    started = time.monotonic()
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, start_new_session=True, cwd=ROOT)
    peak_rss = max_processes = samples = 0
    cpu = {}
    error = None
    # At most one sample per second is retained for the growth experiment.
    series = []
    last_series_second = -1
    try:
        while process.poll() is None:
            snapshot = tree_snapshot(process.pid)
            rss = sum(row[1] for row in snapshot.values()) / 1024
            peak_rss = max(peak_rss, rss)
            max_processes = max(max_processes, len(snapshot))
            for pid, row in snapshot.items():
                cpu[pid] = max(cpu.get(pid, 0), row[2])
            elapsed = time.monotonic() - started
            if int(elapsed) > last_series_second:
                series.append({"seconds": round(elapsed, 3), "rssMiB": round(rss, 3),
                               "processes": len(snapshot)})
                last_series_second = int(elapsed)
            samples += 1
            if elapsed > max_seconds or rss > rss_limit_mib:
                error = "measurement time/RSS safety limit exceeded"
                process.send_signal(signal.SIGINT)
                break
            time.sleep(interval)
        stdout, stderr = process.communicate(timeout=30)
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                # The Go runtime normally cancels and reaps all owned pi workers.
                process.kill()
                process.communicate()
                raise RuntimeError("runtime did not shut down; inspect owned pi processes")
    elapsed = time.monotonic() - started
    if process.returncode or error:
        raise RuntimeError(f"{case}: {error or stderr[-4000:]}")
    report = json.loads(stdout)
    expected = case["roles"] * case["rounds"]
    if (report["stats"]["succeeded"] != expected or report["stats"]["failed"] or
            report["stats"]["rejected"] or report["stats"]["maxActive"] > case["workers"]):
        raise RuntimeError(f"incorrect execution: {report}")
    expected_starts = expected if case.get("fresh") else case["workers"]
    if report["processesStarted"] != expected_starts:
        raise RuntimeError(f"unexpected process restart: {report}")
    return {**case, "elapsedSeconds": elapsed, "tasksPerSecond": expected / elapsed,
            "poolTasksPerSecond": expected / report["poolSeconds"],
            "peakTreeRSSMiB": peak_rss, "sampledCPUSeconds": sum(cpu.values()),
            "meanCPUCores": sum(cpu.values()) / elapsed,
            "maxTreeProcesses": max_processes, "samples": samples,
            "runtime": report, "rssSeries": series}


def recommendation(results, memory_budget, cpu_budget):
    groups = {}
    for row in results:
        if row.get("group") == "workers":
            groups.setdefault(row["workers"], []).append(row)
    candidates = []
    for workers, rows in sorted(groups.items()):
        candidates.append({"workers": workers, "repetitions": len(rows),
            "medianTasksPerSecond": statistics.median(r["tasksPerSecond"] for r in rows),
            "minTasksPerSecond": min(r["tasksPerSecond"] for r in rows),
            "maxTasksPerSecond": max(r["tasksPerSecond"] for r in rows),
            "peakRSSMiB": max(r["peakTreeRSSMiB"] for r in rows),
            "maxMeanCPUCores": max(r["meanCPUCores"] for r in rows),
            "medianExecuteP95MS": statistics.median(r["runtime"]["executeP95Ms"] for r in rows)})
    eligible = [c for c in candidates if c["peakRSSMiB"] <= memory_budget
                and c["maxMeanCPUCores"] <= cpu_budget]
    selected = None
    if eligible:
        best = max(c["medianTasksPerSecond"] for c in eligible)
        selected = min(c["workers"] for c in eligible if c["medianTasksPerSecond"] >= best * .95)
    return {"rule": "smallest measured worker count within 95% of best median end-to-end throughput under budgets",
            "memoryBudgetMiB": memory_budget, "meanCPUBudgetCores": cpu_budget,
            "recommendedWorkers": selected, "candidates": candidates,
            "scope": "synthetic local fixture only; no runtime auto-scaling"}


def plan(kind):
    if kind == "smoke":
        return [{"group": "smoke", "roles": 4, "rounds": 2, "workers": 2}]
    if kind == "wait":
        return [{"group": "wait250", "roles": 40, "rounds": 2, "workers": w, "toolDelayMS": 250}
                for w in [1, 2, 4, 8]]
    if kind == "extended":
        return [
            {"group": "restore", "roles": 400, "rounds": 2, "workers": 4, "payloadBytes": 4096},
            {"group": "history", "roles": 40, "rounds": 20, "workers": 4, "payloadBytes": 4096},
            *[{"group": "baseline", "roles": 40, "rounds": 2, "workers": 4, "fresh": fresh}
              for fresh in [False, True]],
            *[{"group": "wait250", "roles": 40, "rounds": 2, "workers": w, "toolDelayMS": 250}
              for w in [1, 4, 8]],
        ]
    cases = [{"group": "roles", "roles": r, "rounds": 400 // r, "workers": 4}
             for r in [40, 100]]
    for repeat in range(2):
        workers = [1, 2, 4, 8, 12]
        random.Random(71 + repeat).shuffle(workers)
        cases.extend({"group": "workers", "repeat": repeat + 1, "roles": 400,
                      "rounds": 1, "workers": w} for w in workers)
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", choices=["smoke", "matrix", "extended", "wait"], default="matrix")
    parser.add_argument("--workers", help="optional comma-separated filter of worker counts in the selected plan")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/resource-benchmark.json")
    parser.add_argument("--interval", type=float, default=.2)
    parser.add_argument("--memory-budget-mib", type=float)
    parser.add_argument("--cpu-budget-cores", type=float)
    args = parser.parse_args()
    if not .05 <= args.interval <= 5:
        parser.error("interval must be 0.05..5 seconds")
    cases = plan(args.plan)
    if args.workers is not None:
        try:
            selected = {int(w) for w in args.workers.split(",")}
        except ValueError:
            parser.error("workers must be comma-separated integers")
        if not selected or min(selected) < 1 or max(selected) > 64:
            parser.error("workers must be 1..64")
        cases = [case for case in cases if case["workers"] in selected]
        if not cases:
            parser.error("no matching cases in selected plan")
    hw = hardware()
    memory_budget = (args.memory_budget_mib if args.memory_budget_mib is not None
                     else hw["memoryBytes"] / (1024 ** 2) * .25)
    cpu_budget = (args.cpu_budget_cores if args.cpu_budget_cores is not None
                  else hw["logicalCPUs"] * .5)
    if memory_budget <= 0 or cpu_budget <= 0:
        parser.error("budgets must be positive")
    # Fail before launching work when sandbox policy prevents process observation.
    tree_snapshot(os.getpid())
    output = {"schema": 1, "platform": platform.system(), "hardware": hw,
              "sampleIntervalSeconds": args.interval, "plan": args.plan, "workerFilter": args.workers,
              "plannedCases": len(cases), "complete": False,
              "node": subprocess.check_output(["node", "--version"], text=True).strip(),
              "commitBeforeRun": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "worktreeModified": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)),
              "startedUTC": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "results": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    def save():
        output["recommendation"] = recommendation(output["results"], memory_budget, cpu_budget)
        temp = args.output.with_suffix(".tmp")
        temp.write_text(json.dumps(output, indent=2) + "\n")
        temp.replace(args.output)
    save()
    for index, case in enumerate(cases, 1):
        print(f"START {index}/{len(cases)} {json.dumps(case)}", flush=True)
        row = run_case(case, args.interval, 240, hw["memoryBytes"] / 1024 ** 2 * .5)
        output["results"].append(row)
        save()
        print(f"DONE {index}: {row['tasksPerSecond']:.2f} tasks/s, "
              f"{row['peakTreeRSSMiB']:.1f} MiB, {row['meanCPUCores']:.2f} CPU cores", flush=True)
    output["complete"] = True
    save()
    print(json.dumps(output["recommendation"], indent=2), flush=True)


if __name__ == "__main__":
    main()
