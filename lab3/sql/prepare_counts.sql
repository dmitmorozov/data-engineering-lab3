-- Read the same immutable JSONL object directly from S3.
-- source_segment is the independently written variant condition.
CREATE TEMP TABLE vacancies AS
SELECT json_extract_string(vacancy, '$.vacancy_id') AS vacancy_id,
       from_json(json_extract(vacancy, '$.key_skills'), '["VARCHAR"]') AS skills
FROM read_ndjson($uri, columns = {
    source_segment: 'VARCHAR', source_file: 'VARCHAR', vacancy: 'JSON'
})
WHERE source_segment = 'analysts';

-- Keep the first occurrence in source order, then apply the agreed stack cap.
CREATE TEMP TABLE selected AS
WITH exploded AS (
    SELECT vacancy_id, trim(u.skill) AS skill, u.position
    FROM vacancies, unnest(skills) WITH ORDINALITY AS u(skill, position)
    WHERE trim(u.skill) <> ''
),
first_occurrence AS (
    SELECT vacancy_id, skill, min(position) AS position
    FROM exploded GROUP BY vacancy_id, skill
),
ranked AS (
    SELECT *, row_number() OVER (PARTITION BY vacancy_id ORDER BY position) AS rank
    FROM first_occurrence
)
SELECT vacancy_id, skill FROM ranked
WHERE $stack_limit IS NULL OR rank <= $stack_limit;
