WITH purchases AS (
    SELECT item_id, COUNT(*) AS purchases, SUM(item_cost) AS total_gold_spent
    FROM v_fact_match_events
    WHERE event_type = 'item_purchase' AND item_id IS NOT NULL
    GROUP BY item_id
),
totals AS (
    SELECT SUM(purchases) AS all_purchases FROM purchases
)
SELECT
    p.item_id,
    i.item_name,
    i.category,
    p.purchases,
    p.total_gold_spent,
    CAST(p.purchases AS DOUBLE) / NULLIF(t.all_purchases, 0) AS share_of_purchases,
    RANK() OVER (ORDER BY p.purchases DESC) AS purchase_rank
FROM purchases p
JOIN v_dim_item i ON i.item_id = p.item_id
CROSS JOIN totals t
ORDER BY purchase_rank;