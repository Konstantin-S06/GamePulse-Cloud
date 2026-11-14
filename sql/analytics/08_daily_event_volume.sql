SELECT match_date, event_type, COUNT(*) AS events
FROM v_fact_match_events
GROUP BY match_date, event_type
ORDER BY match_date, event_type;