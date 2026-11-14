SELECT
    FLOOR(duration_seconds / 120.0) * 2 AS bucket_start_minutes,
    COUNT(*) AS matches
FROM v_dim_match
WHERE is_complete
GROUP BY bucket_start_minutes
ORDER BY bucket_start_minutes;