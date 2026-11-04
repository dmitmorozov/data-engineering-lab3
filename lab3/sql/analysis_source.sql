-- All original fields are preserved in the JSON payload.
-- This read is independent of frequency result files.
SELECT json_extract_string(vacancy, '$.vacancy_id') AS vacancy_id,
       json_extract_string(vacancy, '$.name') AS vacancy_name,
       json_extract_string(vacancy, '$.area.id') AS area_id,
       json_extract_string(vacancy, '$.area.name') AS area,
       json_extract_string(vacancy, '$.experience.name') AS experience,
       from_json(json_extract(vacancy, '$.key_skills'), '["VARCHAR"]') AS skills,
       json_extract(vacancy, '$.salary') AS salary_json
FROM read_ndjson($uri, columns = {
    source_segment: 'VARCHAR', source_file: 'VARCHAR', vacancy: 'JSON'
})
WHERE source_segment = 'analysts';
