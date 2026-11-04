"""Compare every key and count, not just top rows or totals."""
import argparse
import csv
import json
from pathlib import Path
from common import save_json
from results import read_counts
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    left, right = read_counts(args.left), read_counts(args.right)
    keys = sorted(left.keys() | right.keys(), key=lambda k: (len(k), k))
    diff = [(key, left.get(key), right.get(key)) for key in keys if left.get(key) != right.get(key)]
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / "diff.csv").open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(["skills_json", "left", "right"])
        for key, lvalue, rvalue in diff:
            writer.writerow([json.dumps(key, ensure_ascii=False), lvalue, rvalue])
    summary = {"left": str(args.left), "right": str(args.right),
               "left_keys": len(left), "right_keys": len(right),
               "mismatched_keys": len(diff), "equal": not diff}
    save_json(args.output / "verification.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if diff:
        raise SystemExit(1)
if __name__ == "__main__":
    main()
