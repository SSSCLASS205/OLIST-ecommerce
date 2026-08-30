{{ config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential'],
    incremental_strategy='merge'
) }}

WITH silver_order_payments AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        level_of_installment,
        installment_monthly_value,
        _airbyte_emitted_at
    FROM {{ ref('order_payments_silver') }}
    {% if is_incremental() %}
    WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

joined_data AS (
    SELECT
        a.order_id,
        a.payment_sequential,
        a.payment_installments,
        a.payment_type,
        a.payment_value,
        a.level_of_installment,
        a.installment_monthly_value,
        b.customer_id,
        b.order_approved_at,
        b.order_status,
        a._airbyte_emitted_at
    FROM silver_order_payments a
    JOIN {{ ref('orders_silver') }} b
        ON a.order_id = b.order_id
        AND b.dbt_valid_to IS NULL
)

SELECT
    order_id,
    payment_sequential,
    customer_id,
    payment_installments,
    payment_type,
    payment_value,
    level_of_installment,
    installment_monthly_value,
    COALESCE(TO_NUMBER(TO_CHAR(order_approved_at::DATE, 'YYYYMMDD')), 19000101) AS payment_date_id,
    {{ dbt_utils.generate_surrogate_key(['payment_type', 'order_status']) }} AS attribute_id,
    _airbyte_emitted_at
FROM joined_data