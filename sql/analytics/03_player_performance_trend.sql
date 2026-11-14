WITH player_games AS (
    SELECT player_id, COUNT(*) AS games
    FROM v_fact_player_match
    GROUP BY player_id
),
top_players AS (
    SELECT player_id
    FROM player_games
    ORDER BY games DESC
    LIMIT 10
),
daily AS (
    SELECT
        f.player_id,
        f.match_date,
        COUNT(*) AS games,
        AVG((f.kills + f.assists) / GREATEST(f.deaths, 1)) AS avg_kda,
        AVG(f.damage_dealt) AS avg_damage_dealt
    FROM v_fact_player_match f
    INNER JOIN top_players t ON t.player_id = f.player_id
    GROUP BY f.player_id, f.match_date
)
SELECT player_id, match_date, games, avg_kda, avg_damage_dealt
FROM daily
ORDER BY player_id, match_date;