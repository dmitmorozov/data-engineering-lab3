"""Independent SQL counting over S3; never imports MapReduce."""
import argparse
import json
import time
from pathlib import Path
from common import LAB, config, dataset, profile, save_json, utc_now
from results import write_counts

def connection():
    import duckdb
    cfg = config()
    extensions = LAB / ".runtime" / "duckdb_extensions"
    extensions.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(config={"threads": cfg["duckdb_threads"],
                                "extension_directory": str(extensions)})
    db.execute("LOAD httpfs")
    p = profile()
    endpoint = p["endpoint"].split("://", 1)[-1].rstrip("/")
    quote = lambda s: "'" + str(s).replace("'", "''") + "'"
    # Temporary connection-local secret: no credential values in logs or SQL files.
    db.execute("CREATE SECRET lab3_s3 (TYPE s3, KEY_ID " + quote(p["access_key"]) +
               ", SECRET " + quote(p["secret_key"]) + ", REGION " + quote(p["region"]) +
               ", ENDPOINT " + quote(endpoint) + ", URL_STYLE 'path', USE_SSL " +
               ("true" if p["endpoint"].startswith("https://") else "false") + ")")
    return db

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", choices=["5", "10", "all"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cfg, data = config(), dataset()
    limit = None if args.limit == "all" else int(args.limit)
    db = connection()
    prepare = (LAB / "sql" / "prepare_counts.sql").read_text(encoding="utf-8")
    # Execute statements separately so prepared parameters only apply where used.
    statements = [s.strip() for s in prepare.split(";") if s.strip()]
    started = time.perf_counter()
    db.execute(statements[0], {"uri": data["uri"]})
    db.execute(statements[1], {"stack_limit": limit})
    records = db.execute("SELECT count(*) FROM vacancies").fetchone()[0]
    rows = [(tuple(skills), count) for skills, count in
            db.execute((LAB / "sql" / "counts.sql").read_text(encoding="utf-8")).fetchall()]
    compute_seconds = time.perf_counter() - started
    if records != data["file_count"]:
        raise AssertionError("SQL variant/input count mismatch")
    export_started = time.perf_counter()
    dimensions = write_counts(args.output, rows)
    summary = {"method": "duckdb", "limit": args.limit, "uri": data["uri"],
               "snapshot_sha256": data["jsonl_sha256"], "records": records,
               "threads": cfg["duckdb_threads"], "compute_seconds": compute_seconds,
               "export_seconds": time.perf_counter()-export_started, "at_utc": utc_now(),
               **dimensions}
    save_json(args.output / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    db.close()

if __name__ == "__main__":
    main()
