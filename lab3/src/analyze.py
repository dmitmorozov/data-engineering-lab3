"""Salary associations and two regional views using full skill lists from S3."""
from __future__ import annotations
import csv
import itertools
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from common import LAB, config, dataset, save_json, utc_now
from duckdb_counts import connection
from results import read_counts

def median(values):
    return statistics.median(values) if values else None

def write_csv(path, rows, fields=None):
    rows = list(rows)
    if fields is None:
        if not rows:
            raise ValueError(f"Empty output needs fields: {path}")
        fields = list(rows[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

def skill_json(key):
    return json.dumps(key, ensure_ascii=False, separators=(",", ":"))

def combinations(names):
    for size in (1, 2, 3):
        yield from itertools.combinations(names, size)

def parse_salary(value):
    if not value:
        return None
    low = float(value["from"]) if value.get("from") is not None else None
    high = float(value["to"]) if value.get("to") is not None else None
    if low is None and high is None:
        raise ValueError("Salary object has no bounds")
    if any(x is not None and x <= 0 for x in (low, high)):
        raise ValueError("Non-positive salary bound")
    if low is not None and high is not None and low > high:
        raise ValueError("Inverted salary bounds")
    raw_gross = str(value["gross"]).lower()
    if raw_gross not in ("true", "false"):
        raise ValueError(f"Unexpected gross flag {raw_gross}")
    return {"currency": value["currency"], "gross": raw_gross == "true",
            "value": (low+high)/2 if low is not None and high is not None else low if low is not None else high,
            "complete": low is not None and high is not None, "low": low, "high": high}

def main():
    cfg, data = config(), dataset()
    if cfg["salary_methodology_status"] != "approved":
        raise RuntimeError("Salary methodology not approved")
    out = LAB / "artifacts" / "analysis-20261101" / "results"
    out.mkdir(parents=True, exist_ok=True)
    db = connection()
    fetched = db.execute((LAB / "sql" / "analysis_source.sql").read_text(encoding="utf-8"),
                         {"uri": data["uri"]}).fetchall()
    rows = []
    for identity, name, area_id, area, experience, skills, raw_salary in fetched:
        sal = parse_salary(json.loads(raw_salary) if raw_salary and raw_salary != "null" else None)
        rows.append({"id": identity, "name": name, "area_id": area_id, "area": area,
                     "experience": experience, "skills": tuple(sorted(set(s.strip() for s in skills if s.strip()))),
                     "salary": sal})
    db.close()
    if len(rows) != data["file_count"]:
        raise AssertionError("Analytical input count mismatch")
    global_counts = Counter()
    members = {"net": defaultdict(list), "gross": defaultdict(list)}
    complete_members = {"net": defaultdict(list), "gross": defaultdict(list)}
    region_counts = {region: Counter() for region in cfg["regions"]}
    region_paid = {region: {"net": defaultdict(list), "gross": defaultdict(list)} for region in cfg["regions"]}
    salary_values = {"net": {}, "gross": {}}
    for index, row in enumerate(rows):
        keys = list(combinations(row["skills"]))
        global_counts.update(keys)
        if row["area"] in region_counts:
            region_counts[row["area"]].update(keys)
        sal = row["salary"]
        if sal and sal["currency"] == "RUR":
            group = "gross" if sal["gross"] else "net"
            salary_values[group][index] = sal["value"]
            for key in keys:
                members[group][key].append(index)
                if sal["complete"]:
                    complete_members[group][key].append(index)
                if row["area"] in region_paid:
                    region_paid[row["area"]][group][key].append(sal["value"])
    reference = read_counts(LAB / "artifacts" / "validation-20261101" / "results" / "mrall" / "counts.csv")
    if global_counts != reference:
        raise AssertionError("Analysis full-stack counts differ from independently verified counts")

    def values(group, ids):
        return [salary_values[group][i] for i in ids]
    def baseline(key):
        candidates = [(s,) for s in key] if len(key) == 2 else list(itertools.combinations(key, 2))
        return min(candidates, key=lambda candidate: (-global_counts[candidate], candidate))

    effects, complete_checks = [], []
    group_medians = {group: {key: median(values(group, ids)) for key, ids in groups.items()}
                     for group, groups in members.items()}
    for group in ("net", "gross"):
        for key, ids in sorted(members[group].items(), key=lambda item: (len(item[0]), item[0])):
            if len(key) < 2:
                continue
            base = baseline(key)
            base_ids = members[group][base]
            med, base_med = group_medians[group][key], group_medians[group][base]
            anchor = min(((s,) for s in key), key=lambda k: (-global_counts[k], k))
            exclusive_ids = sorted(set(base_ids) - set(ids))
            complete_ids = complete_members[group][key]
            complete_base_ids = complete_members[group][base]
            complete_med = median(values(group, complete_ids))
            complete_base_med = median(values(group, complete_base_ids))
            effect = {
                "tax_group": group, "kind": len(key), "skills_json": skill_json(key),
                "base_json": skill_json(base), "added_skill": next(s for s in key if s not in base),
                "frequency_all": global_counts[key], "base_frequency_all": global_counts[base],
                "salary_n": len(ids), "base_salary_n": len(base_ids),
                "median_rur": med, "base_median_rur": base_med,
                "delta_rur": med-base_med, "delta_pct": (med/base_med-1)*100,
                "eligible_n5": len(ids) >= cfg["salary_min_support"],
                "eligible_n10": len(ids) >= cfg["salary_sensitivity_support"],
                "base_without_added_n": len(exclusive_ids),
                "base_without_added_median": median(values(group, exclusive_ids)),
                "anchor_skill": anchor[0], "anchor_salary_n": len(members[group][anchor]),
                "anchor_median_rur": group_medians[group][anchor],
                "complete_n": len(complete_ids), "complete_base_n": len(complete_base_ids),
                "complete_median_rur": complete_med, "complete_base_median_rur": complete_base_med,
                "complete_delta_rur": complete_med-complete_base_med if complete_med is not None and complete_base_med is not None else None}
            effects.append(effect)
    write_csv(out / "salary_effects_full.csv", effects)
    rankings = {}
    ranked_flat = []
    for group in ("net", "gross"):
        for size in (2, 3):
            for support in (5, 10):
                candidates = [row for row in effects if row["tax_group"] == group and row["kind"] == size and row["salary_n"] >= support]
                for direction in ("increase", "decrease"):
                    selected = [row for row in candidates if row["delta_rur"] > 0] if direction == "increase" else [row for row in candidates if row["delta_rur"] < 0]
                    selected.sort(key=lambda row: ((-row["delta_rur"] if direction == "increase" else row["delta_rur"]), -row["salary_n"], row["skills_json"]))
                    key = f"{group}_k{size}_n{support}_{direction}"
                    rankings[key] = selected[:10]
                    for rank, effect in enumerate(selected[:10], 1):
                        ranked_flat.append({"ranking": key, "rank": rank, **effect})
    save_json(out / "salary_rankings.json", rankings)
    write_csv(out / "salary_rankings.csv", ranked_flat)

    summaries, demand, strata = [], [], []
    for region in cfg["regions"]:
        subset = [row for row in rows if row["area"] == region]
        nskills = sum(bool(row["skills"]) for row in subset)
        sal_by_group = {g: [row["salary"]["value"] for row in subset if row["salary"] and
                           row["salary"]["currency"] == "RUR" and row["salary"]["gross"] == (g == "gross")] for g in ("net", "gross")}
        summaries.append({"region": region, "vacancies": len(subset), "with_skills": nskills,
                          "without_skills": len(subset)-nskills, "with_any_salary": sum(bool(r["salary"]) for r in subset),
                          "rur_net_n": len(sal_by_group["net"]), "rur_net_median": median(sal_by_group["net"]),
                          "rur_gross_n": len(sal_by_group["gross"]), "rur_gross_median": median(sal_by_group["gross"]),
                          "net_salary_coverage": len(sal_by_group["net"])/len(subset)})
        for key, count in sorted(region_counts[region].items(), key=lambda item: (len(item[0]), -item[1], item[0])):
            net, gross = region_paid[region]["net"][key], region_paid[region]["gross"][key]
            demand.append({"region": region, "kind": len(key), "skills_json": skill_json(key),
                           "vacancies": count, "share_all_vacancies": count/len(subset),
                           "share_with_skills": count/nskills if nskills else None,
                           "rur_net_n": len(net), "rur_net_median": median(net),
                           "rur_gross_n": len(gross), "rur_gross_median": median(gross)})
        for experience, n in Counter(row["experience"] for row in subset).most_common():
            strata.append({"region": region, "experience": experience, "vacancies": n})
    write_csv(out / "region_summary.csv", summaries)
    write_csv(out / "regional_demand.csv", demand)
    write_csv(out / "regional_experience.csv", strata)
    save_json(out / "region_summary.json", summaries)
    # Context for reported net rankings: compare within experience and region when possible.
    context_rows = []
    selected_effects = rankings["net_k2_n5_increase"][:3] + rankings["net_k2_n5_decrease"][:3] + rankings["net_k3_n5_increase"][:3] + rankings["net_k3_n5_decrease"][:3]
    for effect in selected_effects:
        key, base = tuple(json.loads(effect["skills_json"])), tuple(json.loads(effect["base_json"]))
        for axis in ("area", "experience"):
            categories = cfg["regions"] if axis == "area" else sorted(set(r["experience"] for r in rows))
            for category in categories:
                group_ids = [i for i in members["net"][key] if rows[i][axis] == category]
                base_ids = [i for i in members["net"][base] if rows[i][axis] == category]
                med, base_med = median(values("net", group_ids)), median(values("net", base_ids))
                context_rows.append({"skills_json": skill_json(key), "base_json": skill_json(base),
                                     "axis": axis, "category": category, "salary_n": len(group_ids),
                                     "base_salary_n": len(base_ids), "median_rur": med,
                                     "base_median_rur": base_med, "delta_rur": med-base_med if med is not None and base_med is not None else None,
                                     "enough_n5": len(group_ids) >= 5 and len(base_ids) >= 5})
    write_csv(out / "salary_context_checks.csv", context_rows)
    overview = {"at_utc": utc_now(), "uri": data["uri"], "records": len(rows),
                "frequency_keys_equal_to_reference": True,
                "salary_missing": sum(row["salary"] is None for row in rows),
                "foreign_salary": sum(bool(row["salary"]) and row["salary"]["currency"] != "RUR" for row in rows),
                "rur_net": len(salary_values["net"]), "rur_gross": len(salary_values["gross"]),
                "rur_net_with_skills": sum(bool(rows[i]["skills"]) for i in salary_values["net"]),
                "rur_gross_with_skills": sum(bool(rows[i]["skills"]) for i in salary_values["gross"]),
                "global_rur_net_median": median(list(salary_values["net"].values())),
                "global_rur_gross_median": median(list(salary_values["gross"].values())),
                "salary_effect_rows": len(effects),
                "eligible_counts": {f"{group}_k{size}_n{support}": sum(row["tax_group"] == group and row["kind"] == size and row["salary_n"] >= support for row in effects) for group in ("net", "gross") for size in (2, 3) for support in (5, 10)},
                "regional_rows": len(demand)}
    save_json(out / "analysis_summary.json", overview)
    print(json.dumps(overview, ensure_ascii=False, indent=2))
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    print(json.dumps({key: value[:2] for key, value in rankings.items() if key.startswith("net_") and "_n5_" in key}, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
