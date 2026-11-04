"""Deterministic export of analysts only; upload and verify by streaming S3."""
from __future__ import annotations
import hashlib
import json
from collections import Counter
from pathlib import Path
from common import byte_chunks, ROOT, LAB, artifact_dir, config, save_json, s3_client, utc_now

def main():
    source = ROOT / "data" / "analysts"
    generated = LAB / "generated"
    generated.mkdir(parents=True, exist_ok=True)
    output = generated / "vacancies.jsonl"
    entries, ids = [], set()
    regions, skills, currencies, gross, sizes = (Counter() for _ in range(5))
    total_bytes = no_skills = salaries = both = 0
    with output.open("wb") as dest:
        for path in sorted(source.glob("*.json")):
            raw = path.read_bytes()
            vacancy = json.loads(raw.decode("utf-8-sig"))
            identity = str(vacancy["vacancy_id"])
            if identity in ids or identity != path.stem:
                raise ValueError(f"Duplicate/mismatched id: {path.name}")
            ids.add(identity)
            names = vacancy["key_skills"]
            if not isinstance(names, list) or any(not isinstance(s, str) for s in names):
                raise ValueError(f"Unexpected skills schema: {path.name}")
            unique = list(dict.fromkeys(s.strip() for s in names if s.strip()))
            no_skills += not bool(unique)
            sizes[len(unique)] += 1
            skills.update(unique)
            regions[vacancy["area"]["name"]] += 1
            total_bytes += len(raw)
            sal = vacancy.get("salary")
            if sal:
                salaries += 1
                currencies[sal["currency"]] += 1
                gross[str(sal["gross"]).lower()] += 1
                both += bool(unique) and sal["currency"] == "RUR" and str(sal["gross"]).lower() == "false"
            entries.append({"file": path.name, "vacancy_id": identity, "bytes": len(raw),
                            "sha256": hashlib.sha256(raw).hexdigest()})
            # Wrapper preserves every original vacancy field without replacing source values.
            row = {"source_segment": "analysts", "source_file": path.name, "vacancy": vacancy}
            dest.write((json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8"))
    if not entries:
        raise ValueError("No input files")
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    snapshot = digest[:16]
    cfg = config()
    key = f"raw/analysts/{snapshot}/vacancies.jsonl"
    manifest = {"segment": "analysts", "source_directory": "data/analysts",
                "file_count": len(entries), "source_bytes": total_bytes, "files": entries,
                "snapshot_id": snapshot, "jsonl_sha256": digest, "jsonl_bytes": output.stat().st_size,
                "bucket": cfg["bucket"], "key": key, "uri": f"s3://{cfg['bucket']}/{key}"}
    save_json(LAB / "manifests" / "source_files.json", manifest)
    save_json(LAB / "manifests" / "dataset.json", {k: v for k, v in manifest.items() if k != "files"})
    quality = {"count": len(entries), "unique_ids": len(ids), "without_skills": no_skills,
               "with_skills": len(entries)-no_skills, "unique_skills": len(skills),
               "skill_count_distribution": dict(sorted(sizes.items())), "with_salary": salaries,
               "salary_currencies": dict(currencies), "salary_gross": dict(gross),
               "rur_net_with_skills": both, "regions": dict(regions.most_common()),
               "top_skills": skills.most_common(30)}
    save_json(LAB / "manifests" / "quality.json", quality)
    client = s3_client()
    client.upload_file(str(output), cfg["bucket"], key, ExtraArgs={"ContentType": "application/x-ndjson"})
    client.upload_file(str(LAB / "manifests" / "source_files.json"), cfg["bucket"],
                       f"metadata/analysts/{snapshot}/manifest.json", ExtraArgs={"ContentType": "application/json"})
    hash_remote, bytes_remote, rows_remote = hashlib.sha256(), 0, 0
    response = client.get_object(Bucket=cfg["bucket"], Key=key)
    with response["Body"] as body:
        for chunk in byte_chunks(body, 262144):
            hash_remote.update(chunk)
            bytes_remote += len(chunk)
            rows_remote += chunk.count(b"\n")
    assert hash_remote.hexdigest() == digest
    assert bytes_remote == output.stat().st_size
    assert rows_remote == len(entries)
    evidence = {"at_utc": utc_now(), "uri": manifest["uri"], "snapshot_id": snapshot,
                "local_sha256": digest, "remote_sha256": hash_remote.hexdigest(),
                "bytes": bytes_remote, "records": rows_remote, "equal": True}
    save_json(artifact_dir() / "results" / "dataset_upload.json", evidence)
    print(json.dumps(evidence, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
