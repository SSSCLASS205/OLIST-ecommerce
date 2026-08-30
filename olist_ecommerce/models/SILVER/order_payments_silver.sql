{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_id', 'payment_sequential']
) }}

WITH order_payments_bronze AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        _airbyte_emitted_at
    FROM {{ source('BRONZE', 'order_payments_bronze') }}
    {% if is_incremental() %}
    WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *
    FROM order_payments_bronze
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id, payment_sequential
        ORDER BY _airbyte_emitted_at DESC
    ) = 1
),

transformed AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        _airbyte_emitted_at,
        CASE
            WHEN payment_installments = 0 THEN 'NONE'
            WHEN payment_installments = 1 THEN 'LOW'
            WHEN payment_installments < 4 THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS level_of_installment,
        CASE
            WHEN payment_installments > 0 THEN payment_value / payment_installments
            ELSE payment_value
        END AS installment_monthly_value
    FROM deduped
)

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    level_of_installment,
    installment_monthly_value,
    _airbyte_emitted_at
FROM transformed