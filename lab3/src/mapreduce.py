"""Own streaming Map/Shuffle/Reduce with bounded inflight work and process recovery."""
from __future__ import annotations
import argparse
import hashlib
import itertools
import json
import os
import time
from collections import Counter
import multiprocessing
from multiprocessing.connection import wait as wait_connections
from pathlib import Path
from common import byte_chunks, config, dataset, s3_client, save_json, utc_now
from results import write_counts

def record_batches(chunks, batch_records, stats):
    """Split raw bytes on LF before UTF-8 decoding: multibyte characters stay intact."""
    pending, batch = b"", []
    digest = hashlib.sha256()
    for chunk in chunks:
        digest.update(chunk)
        stats["read_bytes"] += len(chunk)
        stats["network_chunks"] += 1
        lines = (pending + chunk).split(b"\n")
        pending = lines.pop()
        for line in lines:
            if not line.strip():
                continue
            batch.append(json.loads(line.decode("utf-8")))
            if len(batch) >= batch_records:
                yield batch
                batch = []
    if pending.strip():
        batch.append(json.loads(pending.decode("utf-8")))
    if batch:
        yield batch
    stats["sha256"] = digest.hexdigest()

def map_batch(job):
    chunk_id, records, limit, combine, crash = job
    if crash:
        os._exit(70)  # Real mapper process failure, not a simulated return value.
    emissions, selected, accepted = [0, 0, 0], 0, 0
    output = Counter() if combine else []
    for record in records:
        if record.get("source_segment") != "analysts":
            continue
        accepted += 1
        names = list(dict.fromkeys(s.strip() for s in record["vacancy"]["key_skills"] if s.strip()))
        names = sorted(names[:limit] if limit is not None else names)
        selected += len(names)
        for size in (1, 2, 3):
            for key in itertools.combinations(names, size):
                emissions[size-1] += 1
                if combine:
                    output[key] += 1
                else:
                    output.append((key, 1))
    items = list(output.items()) if combine else output
    return {"chunk_id": chunk_id, "items": items, "emissions": emissions,
            "selected_skills": selected, "records": accepted, "pid": os.getpid()}

def _worker_loop(connection):
    """One task at a time over an explicit pipe; the coordinator owns retries."""
    try:
        while True:
            job = connection.recv()
            if job is None:
                return
            try:
                connection.send({"result": map_batch(job)})
            except Exception as error:
                connection.send({"error": repr(error)})
    except EOFError:
        return
    finally:
        connection.close()

def calculate(chunks, limit, combine=False, workers=2, batch_records=128,
              max_inflight=4, fail_chunk=None, event=lambda value: None):
    stats = {"read_bytes": 0, "network_chunks": 0, "sha256": None,
             "records": 0, "batches": 0, "emissions": [0, 0, 0], "shuffle_records": 0,
             "recoveries": 0, "committed_windows": 0}
    counts = Counter()
    batches = enumerate(record_batches(chunks, batch_records, stats))
    context = multiprocessing.get_context("spawn")
    states = []
    failure_injected = False

    def start_worker():
        parent, child = context.Pipe()
        process = context.Process(target=_worker_loop, args=(child,))
        process.start()
        child.close()
        return {"process": process, "connection": parent, "job": None}

    def stop_worker(state):
        process, connection = state["process"], state["connection"]
        if process.is_alive():
            if state["job"] is None:
                try:
                    connection.send(None)
                except (OSError, EOFError):
                    pass
            else:
                process.terminate()
        process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join()
        connection.close()

    try:
        states = [start_worker() for _ in range(workers)]
        while True:
            window = list(itertools.islice(batches, max_inflight))
            if not window:
                break
            pending = list(window)
            attempts, outputs = {}, {}
            while pending or any(state["job"] is not None for state in states):
                for state in states:
                    if state["job"] is not None or not pending:
                        continue
                    chunk_id, records = pending.pop(0)
                    attempts[chunk_id] = attempts.get(chunk_id, 0) + 1
                    if attempts[chunk_id] > 3:
                        raise RuntimeError(f"Mapper retries exhausted for chunk {chunk_id}")
                    crash = chunk_id == fail_chunk and not failure_injected
                    if crash:
                        failure_injected = True
                        event({"event": "inject_mapper_exit", "chunk_id": chunk_id, "exit_code": 70})
                    state["job"] = (chunk_id, records)
                    state["connection"].send((chunk_id, records, limit, combine, crash))
                active = [state for state in states if state["job"] is not None]
                ready = wait_connections([state["connection"] for state in active])
                for state in active:
                    if state["connection"] not in ready:
                        continue
                    chunk_id, records = state["job"]
                    try:
                        message = state["connection"].recv()
                    except (EOFError, OSError):
                        state["process"].join(timeout=5)
                        exit_code = state["process"].exitcode
                        event({"event": "mapper_process_failure", "chunk_id": chunk_id,
                               "attempt": attempts[chunk_id], "exit_code": exit_code,
                               "replay_chunks": [chunk_id]})
                        stats["recoveries"] += 1
                        stop_worker(state)
                        state.update(start_worker())
                        pending.insert(0, (chunk_id, records))
                        continue
                    state["job"] = None
                    if "error" in message:
                        event({"event": "mapper_task_failure", "chunk_id": chunk_id,
                               "error": message["error"], "attempt": attempts[chunk_id]})
                        stats["recoveries"] += 1
                        pending.insert(0, (chunk_id, records))
                        continue
                    result = message["result"]
                    if result["chunk_id"] != chunk_id or chunk_id in outputs:
                        raise AssertionError("Unexpected or duplicate mapper result")
                    outputs[chunk_id] = result
            # Transactional window commit: no failed/partial attempt contributes.
            for chunk_id, _ in window:
                result = outputs[chunk_id]
                for key, count in result["items"]:
                    counts[key] += count
                stats["records"] += result["records"]
                stats["batches"] += 1
                stats["shuffle_records"] += len(result["items"])
                stats["emissions"] = [a+b for a, b in zip(stats["emissions"], result["emissions"])]
                event({"event": "reduce_commit", "chunk_id": result["chunk_id"],
                       "mapper_pid": result["pid"], "records": result["records"],
                       "emissions": result["emissions"], "shuffle_records": len(result["items"])})
            stats["committed_windows"] += 1
    finally:
        for state in states:
            stop_worker(state)
    if fail_chunk is not None and not failure_injected:
        raise ValueError("Requested failure chunk was not reached")
    return counts, stats

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", choices=["5", "10", "all"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--combine", action="store_true")
    parser.add_argument("--fail-chunk", type=int)
    args = parser.parse_args()
    cfg, data = config(), dataset()
    args.output.mkdir(parents=True, exist_ok=True)
    limit = None if args.limit == "all" else int(args.limit)
    # Credential discovery and HTTP client initialization are outside measured computation.
    client = s3_client()
    with (args.output / "events.jsonl").open("w", encoding="utf-8") as log:
        def event(value):
            log.write(json.dumps({"at_utc": utc_now(), **value}, ensure_ascii=False) + "\n")
            log.flush()
        started = time.perf_counter()
        response = client.get_object(Bucket=data["bucket"], Key=data["key"])
        with response["Body"] as body:
            counts, stats = calculate(
                byte_chunks(body, cfg["chunk_bytes"]), limit, args.combine,
                cfg["workers"], cfg["batch_records"], cfg["max_inflight"],
                args.fail_chunk, event)
        compute_seconds = time.perf_counter() - started
    if stats["sha256"] != data["jsonl_sha256"] or stats["records"] != data["file_count"]:
        raise AssertionError("Input hash or record count mismatch")
    export_started = time.perf_counter()
    dimensions = write_counts(args.output, counts.items())
    summary = {"method": "mapreduce_combiner" if args.combine else "mapreduce",
               "limit": args.limit, "uri": data["uri"], "at_utc": utc_now(),
               "compute_seconds": compute_seconds,
               "export_seconds": time.perf_counter()-export_started,
               "workers": cfg["workers"], "batch_records": cfg["batch_records"],
               "chunk_bytes": cfg["chunk_bytes"], "max_inflight": cfg["max_inflight"],
               **stats, **dimensions}
    save_json(args.output / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
