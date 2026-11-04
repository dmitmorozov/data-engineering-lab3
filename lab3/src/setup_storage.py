"""Start the existing six-volume RustFS without changing ACLs or disk layout."""
from __future__ import annotations
import json
import os
import platform
import socket
import subprocess
import sys
import time
from pathlib import Path
from common import ROOT, LAB, artifact_dir, config, profile, s3_client, save_json, utc_now

def port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False

def command_log(args, destination):
    result = subprocess.run(args, capture_output=True, encoding="utf-8", errors="replace")
    save_json(destination, {"command": args, "exit_code": result.returncode,
                            "stdout": result.stdout, "stderr": result.stderr, "at_utc": utc_now()})
    if result.returncode:
        raise RuntimeError(f"Command failed; see {destination}")
    return result.stdout

def main():
    import psutil
    import boto3
    import duckdb
    art = artifact_dir()
    cfg = config()
    volumes = [ROOT / "disks" / f"disk{i}" for i in range(1, 7)]
    for volume in volumes:
        if not volume.is_dir():
            raise RuntimeError(f"Expected existing volume: {volume}")
        # Check actual write access, without changing permissions.
        probe = volume / ".lab3-access-probe"
        with probe.open("xb") as target:
            target.write(b"lab3")
        probe.unlink()
    acl_script = "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; Get-ChildItem -LiteralPath 'D:/rustfs-lab/disks' -Directory | ForEach-Object { $a=Get-Acl -LiteralPath $_.FullName; [pscustomobject]@{Name=$_.Name;Access=$a.AccessToString} } | ConvertTo-Json -Depth 3"
    command_log(["powershell.exe", "-NoProfile", "-Command", acl_script], art / "logs" / "disk_permissions.json")
    save_json(art / "results" / "environment.json", {
        "at_utc": utc_now(), "platform": platform.platform(), "python": sys.version,
        "cpu_logical": psutil.cpu_count(), "cpu_physical": psutil.cpu_count(logical=False),
        "ram_bytes": psutil.virtual_memory().total, "boto3": boto3.__version__,
        "duckdb": duckdb.__version__, "psutil": psutil.__version__,
        "volumes": list(map(str, volumes)), "same_physical_disk": True,
        "erasure": {"data": 3, "parity": 3}})
    for program in ("rustfs", "rc"):
        command_log([str(ROOT / "bin" / f"{program}.exe"), "--version"],
                    art / "logs" / f"{program}_version.json")
    if not port_open(9000):
        p = profile()
        env = os.environ.copy()
        env.update(RUSTFS_ERASURE_SET_DRIVE_COUNT="6",
                   RUSTFS_STORAGE_CLASS_STANDARD="EC:3",
                   RUSTFS_UNSAFE_BYPASS_DISK_CHECK="true",
                   RUSTFS_ACCESS_KEY=p["access_key"], RUSTFS_SECRET_KEY=p["secret_key"])
        args = [str(ROOT / "bin" / "rustfs.exe"), "server",
                "--address", "127.0.0.1:9000", "--console-enable",
                "--console-address", "127.0.0.1:9001", *map(str, volumes)]
        with (art / "logs" / "rustfs.stdout.log").open("ab") as out, (art / "logs" / "rustfs.stderr.log").open("ab") as err:
            proc = subprocess.Popen(args, cwd=str(ROOT), env=env, stdout=out, stderr=err,
                                    creationflags=subprocess.CREATE_NO_WINDOW)
        save_json(LAB / ".runtime" / "rustfs.json", {"pid": proc.pid, "started_at_utc": utc_now()})
        save_json(art / "logs" / "server_start.json",
                  {"command": args, "environment": {
                      k: env[k] for k in ("RUSTFS_ERASURE_SET_DRIVE_COUNT",
                                         "RUSTFS_STORAGE_CLASS_STANDARD", "RUSTFS_UNSAFE_BYPASS_DISK_CHECK")},
                   "pid": proc.pid, "at_utc": utc_now()})
        for _ in range(90):
            if port_open(9000):
                break
            if proc.poll() is not None:
                raise RuntimeError("RustFS exited; see server logs")
            time.sleep(1)
        else:
            raise RuntimeError("RustFS did not open port 9000")
    # Admin info is evidence of actual disk availability, not just open ports.
    admin_args = [str(ROOT / "bin" / "rc.exe"), "admin", "info", "cluster", cfg["alias"], "--json"]
    for attempt in range(60):
        ready = subprocess.run(admin_args, capture_output=True, encoding="utf-8", errors="replace")
        if ready.returncode == 0:
            break
        time.sleep(1)
    raw = command_log(admin_args, art / "logs" / "cluster.json")
    cluster = json.loads(raw)
    if cluster["onlineDisks"] != 6 or cluster["offlineDisks"] != 0:
        raise RuntimeError("Expected all six disks online before proceeding")
    print(raw)
    client = s3_client()
    buckets = [x["Name"] for x in client.list_buckets()["Buckets"]]
    if cfg["bucket"] not in buckets:
        client.create_bucket(Bucket=cfg["bucket"])
    key = "checks/lab3-read-write.txt"
    body = b"lab3 S3 read/write probe\n"
    client.put_object(Bucket=cfg["bucket"], Key=key, Body=body)
    got = client.get_object(Bucket=cfg["bucket"], Key=key)
    with got["Body"] as stream:
        assert stream.read(1024) == body
    evidence = {"at_utc": utc_now(), "bucket": cfg["bucket"], "key": key,
                "read_write_equal": True, "bytes": len(body),
                "buckets": [x["Name"] for x in client.list_buckets()["Buckets"]]}
    save_json(art / "results" / "storage_check.json", evidence)
    print(json.dumps(evidence, ensure_ascii=False))

if __name__ == "__main__":
    main()
