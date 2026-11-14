WITH per_match AS (
    SELECT match_date, match_id, SUM(damage_dealt) AS match_damage
    FROM v_fact_player_match
    GROUP BY match_date, match_id
),
per_date_match AS (
    SELECT match_date, COUNT(*) AS matches, AVG(match_damage) AS avg_damage_per_match
    FROM per_match
    GROUP BY match_date
),
per_player AS (
    SELECT match_date, AVG(damage_dealt) AS avg_damage_per_player
    FROM v_fact_player_match
    GROUP BY match_date
)
SELECT
    d.match_date,
    d.matches,
    d.avg_damage_per_match,
    p.avg_damage_per_player
FROM per_date_match d
JOIN per_player p ON p.match_date = d.match_date
ORDER BY d.match_date;