-- Fails if fact_sales contains orders that don't exist in orders_silver
-- (would indicate a join exploded rows or pulled in orphaned data).
SELECT f.order_id
FROM {{ ref('fact_sales') }} f
LEFT JOIN {{ ref('orders_silver') }} o ON f.order_id = o.order_id
WHERE o.order_id IS NULL
