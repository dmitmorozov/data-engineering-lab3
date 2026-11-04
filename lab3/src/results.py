"""Serialization of results; no shared computation between MR and SQL."""
import csv
import json
from pathlib import Path
from common import save_json

def write_counts(destination, rows):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    unique, emissions = [0, 0, 0], [0, 0, 0]
    with (destination / "counts.csv").open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(["kind", "skills_json", "count"])
        for skills, count in sorted(rows, key=lambda x: (len(x[0]), x[0])):
            size = len(skills)
            writer.writerow([size, json.dumps(skills, ensure_ascii=False, separators=(",", ":")), count])
            unique[size-1] += 1
            emissions[size-1] += count
    return {"unique_keys": unique, "emissions": emissions}

def read_counts(path):
    with Path(path).open(encoding="utf-8", newline="") as source:
        return {tuple(json.loads(row["skills_json"])): int(row["count"]) for row in csv.DictReader(source)}
