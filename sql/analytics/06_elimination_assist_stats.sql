WITH base AS (
    SELECT
        c.role,
        f.kills,
        f.assists,
        f.deaths,
        d.duration_seconds
    FROM v_fact_player_match f
    JOIN v_dim_character c ON c.character_id = f.character_id
    JOIN v_dim_match d ON d.match_id = f.match_id
)
SELECT
    role,
    COUNT(*) AS player_rows,
    AVG(kills) AS avg_kills,
    AVG(assists) AS avg_assists,
    AVG(deaths) AS avg_deaths,
    SUM(kills) / NULLIF(SUM(duration_seconds), 0) * 60 AS kills_per_minute
FROM base
GROUP BY role
ORDER BY role;