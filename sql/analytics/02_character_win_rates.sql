WITH stats AS (
    SELECT
        f.character_id,
        COUNT(*) AS games,
        SUM(CASE WHEN f.won THEN 1 ELSE 0 END) AS wins
    FROM v_fact_player_match f
    GROUP BY f.character_id
)
SELECT
    s.character_id,
    c.character_name,
    c.role,
    s.games,
    s.wins,
    CAST(s.wins AS DOUBLE) / NULLIF(s.games, 0) AS win_rate
FROM stats s
JOIN v_dim_character c ON c.character_id = s.character_id
ORDER BY win_rate DESC;