"""Save observed sources of uncertainty without modifying or filtering the variant."""
import json
from common import LAB, dataset, save_json
from duckdb_counts import connection
db = connection()
rows = db.execute("""
SELECT json_extract_string(vacancy, '$.vacancy_id'),
       json_extract_string(vacancy, '$.name'),
       json_extract_string(vacancy, '$.area.name'),
       json_extract_string(vacancy, '$.employer_id'),
       json_extract_string(vacancy, '$.experience.name'),
       from_json(json_extract(vacancy, '$.key_skills'), '["VARCHAR"]'),
       json_extract(vacancy, '$.salary')
FROM read_ndjson($uri, columns={source_segment:'VARCHAR',source_file:'VARCHAR',vacancy:'JSON'})
WHERE source_segment='analysts'
""", {"uri": dataset()["uri"]}).fetchall()
items = [dict(zip(["id","name","area","employer_id","experience","skills","salary"], row)) for row in rows]
for row in items:
    row["salary"] = json.loads(row["salary"]) if row["salary"] and row["salary"] != "null" else None
rest_sql = [row for row in items if {"REST","SQL"}.issubset(set(row["skills"])) and row["salary"] and row["salary"]["currency"]=="RUR" and str(row["salary"]["gross"]).lower()=="false"]
long_gross = [row for row in items if len(row["skills"])>=25 and row["salary"] and row["salary"]["currency"]=="RUR" and str(row["salary"]["gross"]).lower()=="true"]
result = {"rest_sql_rur_net":rest_sql,"gross_with_at_least_25_skills":long_gross,
          "long_gross_count":len(long_gross),"long_gross_unique_employers":len(set(row["employer_id"] for row in long_gross))}
save_json(LAB/"artifacts"/"analysis-20261101"/"results"/"observed_patterns.json", result)
print(json.dumps({k:v for k,v in result.items() if not isinstance(v,list)},ensure_ascii=False))
