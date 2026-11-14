WITH stats AS (
    SELECT
        f.character_id,
        COUNT(*) AS games,
        AVG(f.damage_dealt) AS avg_damage_dealt,
        AVG(f.damage_received) AS avg_damage_received,
        AVG(f.gold_spent) AS avg_gold_spent,
        AVG((f.kills + f.assists) / GREATEST(f.deaths, 1)) AS avg_kda
    FROM v_fact_player_match f
    GROUP BY f.character_id
)
SELECT
    s.character_id,
    c.character_name,
    s.games,
    s.avg_damage_dealt,
    s.avg_damage_received,
    s.avg_gold_spent,
    s.avg_kda,
    RANK() OVER (ORDER BY s.avg_damage_dealt DESC) AS damage_rank,
    RANK() OVER (ORDER BY s.avg_kda DESC) AS kda_rank
FROM stats s
JOIN v_dim_character c ON c.character_id = s.character_id
ORDER BY damage_rank;