{{ config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential']  
) }}

with order_payments_bronze as (
    select 
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        _airbyte_emitted_at,
        CASE
            WHEN payment_installments = 1 THEN 'LOW'
            WHEN payment_installments < 4 THEN 'MEDIUM'
            ELSE 'HIGH'
        end as level_of_installment,
        CASE 
            WHEN payment_installments > 0 THEN payment_value / payment_installments
            ELSE payment_value 
        END as installment_monthly_value
    from {{ source('BRONZE', 'order_payments_bronze') }}
    
    {% if is_incremental() %}
        WHERE _airbyte_emitted_at > (select MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

ranked_orders AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY order_id, payment_sequential ORDER BY _airbyte_emitted_at DESC) as rnk
    FROM order_payments_bronze
),

deduped AS (
    SELECT * FROM ranked_orders
    WHERE rnk = 1
)   

select 
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    level_of_installment,
    installment_monthly_value,
    _airbyte_emitted_at
from deduped;