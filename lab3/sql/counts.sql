-- Lexical comparisons enumerate each unordered combination once.
SELECT [skill] AS skills, count(*)::BIGINT AS frequency
FROM selected GROUP BY skill
UNION ALL
SELECT [a.skill, b.skill] AS skills, count(*)::BIGINT AS frequency
FROM selected a JOIN selected b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
GROUP BY a.skill, b.skill
UNION ALL
SELECT [a.skill, b.skill, c.skill] AS skills, count(*)::BIGINT AS frequency
FROM selected a
JOIN selected b ON a.vacancy_id = b.vacancy_id AND a.skill < b.skill
JOIN selected c ON a.vacancy_id = c.vacancy_id AND b.skill < c.skill
GROUP BY a.skill, b.skill, c.skill;
