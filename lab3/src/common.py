"""Shared paths and access only; counting logic stays independent."""
from __future__ import annotations
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LAB = ROOT / "lab3"
CONFIG_PATH = LAB / "configs" / "experiment.json"

def config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def artifact_dir(run_id="setup-20261101"):
    dest = LAB / "artifacts" / run_id
    for part in ("logs", "results", "profiling", "screenshots", "plots"):
        (dest / part).mkdir(parents=True, exist_ok=True)
    return dest

def profile():
    cfg = config()
    access, secret = os.getenv("LAB3_S3_ACCESS_KEY"), os.getenv("LAB3_S3_SECRET_KEY")
    if access and secret:
        return dict(endpoint=os.getenv("LAB3_S3_ENDPOINT", cfg["endpoint"]),
                    region=cfg["region"], access_key=access, secret_key=secret)
    # Capture in memory: never print or persist exported credentials.
    result = subprocess.run(
        [str(ROOT / "bin" / "rc.exe"), "alias", "export", cfg["alias"],
         "--include-credentials", "--acknowledge-credentials"],
        capture_output=True, check=True, encoding="utf-8")
    alias = json.loads(result.stdout)["aliases"][0]
    return dict(endpoint=alias["endpoint"], region=alias.get("region") or cfg["region"],
                **alias["credentials"])

def s3_client():
    import boto3
    from botocore.config import Config
    p = profile()
    return boto3.client("s3", endpoint_url=p["endpoint"], region_name=p["region"],
                        aws_access_key_id=p["access_key"],
                        aws_secret_access_key=p["secret_key"],
                        config=Config(s3={"addressing_style": "path"},
                                      retries={"max_attempts": 3, "mode": "standard"},
                                      connect_timeout=5, read_timeout=30))

def dataset():
    return json.loads((LAB / "manifests" / "dataset.json").read_text(encoding="utf-8"))

def byte_chunks(stream, size):
    """Bounded reads supported by StreamingBody and HTTPResponse alike."""
    while True:
        chunk = stream.read(size)
        if not chunk:
            return
        yield chunk
