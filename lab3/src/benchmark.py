"""Sequential, interleaved fresh-process trials; sample the whole client tree."""
from __future__ import annotations
import csv
import hashlib
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
import psutil
from common import ROOT, LAB, config, dataset, save_json, utc_now

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    cfg, data = config(), dataset()
    destination = LAB / "artifacts" / "benchmark-20261101"
    logs = destination / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    code_version = subprocess.check_output(
        ["git", "-c", f"safe.directory={ROOT.as_posix()}", "rev-parse", "HEAD"],
        cwd=ROOT, encoding="utf-8").strip()
    methods = ["mapreduce", "duckdb", "mapreduce_combiner"]
    trials = []
    for limit in ("5", "10", "all"):
        reference_hash = digest(LAB / "artifacts" / "validation-20261101" / "results" / f"mr{limit}" / "counts.csv")
        for repetition in range(1, cfg["attempts"]+1):
            # Rotate order; no competing analytical runs, no forced OS cache eviction.
            order = methods[repetition-1:] + methods[:repetition-1]
            for method in order:
                trial_id = f"{method}-k{limit}-r{repetition}"
                output = destination / "results" / trial_id
                output.mkdir(parents=True, exist_ok=True)
                script = "duckdb_counts.py" if method == "duckdb" else "mapreduce.py"
                command = [sys.executable, "-X", "utf8", str(LAB / "src" / script),
                           "--limit", limit, "--output", str(output)]
                if method == "mapreduce_combiner":
                    command.append("--combine")
                peak_rss = peak_private = samples = 0
                with (logs / f"{trial_id}.stdout.log").open("wb") as stdout, (logs / f"{trial_id}.stderr.log").open("wb") as stderr:
                    started = time.perf_counter()
                    process = subprocess.Popen(command, cwd=ROOT, stdout=stdout, stderr=stderr)
                    root = psutil.Process(process.pid)
                    while process.poll() is None:
                        rss = private = 0
                        try:
                            tree = [root, *root.children(recursive=True)]
                        except psutil.NoSuchProcess:
                            tree = []
                        for member in tree:
                            try:
                                info = member.memory_info()
                                rss += info.rss
                                private += getattr(info, "private", 0)
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
                        peak_rss = max(peak_rss, rss)
                        peak_private = max(peak_private, private)
                        samples += 1
                        time.sleep(0.01)
                    wall_seconds = time.perf_counter() - started
                if process.returncode:
                    raise RuntimeError(f"Trial {trial_id} failed: see {logs}")
                summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
                actual_hash = digest(output / "counts.csv")
                if actual_hash != reference_hash:
                    raise AssertionError(f"Incorrect trial {trial_id}")
                trial = {"trial_id": trial_id, "method": method, "limit": limit,
                         "repetition": repetition, "wall_seconds": wall_seconds,
                         "compute_seconds": summary["compute_seconds"],
                         "export_seconds": summary["export_seconds"],
                         "peak_tree_rss_bytes": peak_rss, "peak_tree_private_bytes": peak_private,
                         "samples": samples, "sampling_interval_seconds": 0.01,
                         "counts_sha256": actual_hash, "equal_to_reference": True,
                         "shuffle_records": summary.get("shuffle_records"),
                         "code_commit": code_version, "input_sha256": data["jsonl_sha256"],
                         "at_utc": utc_now()}
                trials.append(trial)
                save_json(output / "trial.json", {**trial, "command": command})
                # Canonical full counts retained in validation and S3. Keep hashes for each repeat.
                (output / "counts.csv").unlink()
                print(json.dumps(trial, ensure_ascii=False), flush=True)
                save_json(destination / "profiling" / "runs.json", trials)
    profiling = destination / "profiling"
    with (profiling / "benchmark_runs.csv").open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(trials[0]))
        writer.writeheader()
        writer.writerows(trials)
    aggregated = []
    for limit in ("5", "10", "all"):
        for method in methods:
            group = [r for r in trials if r["limit"] == limit and r["method"] == method]
            aggregated.append({"limit": limit, "method": method, "attempts": len(group),
                **{f"{field}_{stat}": function([r[field] for r in group])
                   for field in ("wall_seconds", "compute_seconds", "peak_tree_rss_bytes", "peak_tree_private_bytes")
                   for stat, function in (("median", statistics.median), ("min", min), ("max", max))}})
    save_json(profiling / "summary.json", aggregated)
    with (profiling / "summary.csv").open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(aggregated[0]))
        writer.writeheader()
        writer.writerows(aggregated)
    save_json(profiling / "methodology.json", {
        "scope": "Fresh process client tree; RustFS excluded; shared RSS pages may be counted across processes",
        "wall_timer": "Process creation through exit, includes imports/client setup, S3, computation and local export",
        "compute_timer": "S3 GET/SQL materialization through final counts, includes mapper startup; excludes client setup and CSV export",
        "memory_timer": "Entire fresh-process lifetime; sampled every 10 ms; may miss shorter peaks",
        "cache": "OS/server caches uncontrolled and likely warm; no dedicated warmup; method order rotated each repetition",
        "repetitions": cfg["attempts"], "workers_and_sql_threads": 2,
        "optimization": "Mapper-local combiner, all other parameters fixed",
        "code_commit": code_version, "input_sha256": data["jsonl_sha256"]})

if __name__ == "__main__":
    main()
