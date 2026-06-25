-- Fails if any order claims to be delivered to the customer before it
-- was even purchased — a sign of upstream data corruption or a bad join.
SELECT
    order_id,
    order_purchase_timestamp,
    order_delivered_customer_date
FROM {{ ref('orders_silver') }}
WHERE order_delivered_customer_date IS NOT NULL
  AND order_delivered_customer_date < order_purchase_timestamp
