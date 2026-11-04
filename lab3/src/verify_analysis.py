"""Independent SQL checks of Python medians and salary sample sizes."""
import json
from common import LAB, dataset, save_json
from duckdb_counts import connection
out = LAB / "artifacts" / "analysis-20261101" / "results"
db = connection()
source = (LAB / "sql" / "analysis_source.sql").read_text(encoding="utf-8").strip().rstrip(";")
db.execute("CREATE TEMP TABLE source AS " + source, {"uri": dataset()["uri"]})
db.execute("""
CREATE TEMP TABLE paid AS
WITH bounds AS (
 SELECT *, try_cast(json_extract_string(salary_json, '$.from') AS DOUBLE) AS low,
           try_cast(json_extract_string(salary_json, '$.to') AS DOUBLE) AS high
 FROM source WHERE json_extract_string(salary_json, '$.currency') = 'RUR'
)
SELECT *, coalesce((low+high)/2, low, high) AS representative,
       lower(json_extract_string(salary_json, '$.gross')) AS gross_flag
FROM bounds;
""")
checks = []
for region in json.loads((out / "region_summary.json").read_text(encoding="utf-8")):
    actual = db.execute("""
    SELECT count(*), count(*) FILTER (WHERE array_length(skills)>0)
    FROM source WHERE area = ?
    """, [region["region"]]).fetchone()
    assert actual == (region["vacancies"], region["with_skills"])
    for group in ("net", "gross"):
        n, med = db.execute("""
        SELECT count(*), median(representative)
        FROM paid WHERE area = ? AND gross_flag = ?
        """, [region["region"], "false" if group == "net" else "true"]).fetchone()
        assert n == region[f"rur_{group}_n"] and med == region[f"rur_{group}_median"]
        checks.append({"region": region["region"], "group": group, "n": n, "median": med, "equal": True})
rankings = json.loads((out / "salary_rankings.json").read_text(encoding="utf-8"))
for name, effects in rankings.items():
    for effect in effects[:2]:
        for field, nfield, mfield in (("skills_json", "salary_n", "median_rur"),
                                     ("base_json", "base_salary_n", "base_median_rur")):
            skills = json.loads(effect[field])
            n, med = db.execute("""
              SELECT count(*), median(representative)
              FROM paid WHERE gross_flag = ? AND list_has_all(skills, ?)
              """, ["false" if effect["tax_group"] == "net" else "true", skills]).fetchone()
            assert n == effect[nfield] and med == effect[mfield], (name, effect[field], n, med)
checks.append({"salary_rankings_checked": sum(min(2,len(v))*2 for v in rankings.values()), "equal": True})
save_json(out / "analysis_verification.json", {"equal": True, "checks": checks})
print(json.dumps({"equal": True, "checks": checks}, ensure_ascii=False, indent=2))
