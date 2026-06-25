{{ config(
    materialized='table'
) }}

WITH distinct_payments AS (
    SELECT DISTINCT payment_type 
    FROM {{ ref('order_payments_silver') }}
    WHERE payment_type IS NOT NULL
),

distinct_statuses AS (
    SELECT DISTINCT order_status 
    FROM {{ ref('orders_silver') }}
    WHERE order_status IS NOT NULL
),

cartesian_product AS (
    SELECT 
        p.payment_type,
        s.order_status
    FROM distinct_payments p
    CROSS JOIN distinct_statuses s
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['payment_type', 'order_status']) }} as attribute_id,
    payment_type,
    order_status
FROM cartesian_product