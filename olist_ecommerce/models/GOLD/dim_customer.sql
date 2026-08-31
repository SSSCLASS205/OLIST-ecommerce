{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        incremental_strategy='merge'
    )
}}

WITH changed_customers AS (

    SELECT DISTINCT customer_id
    FROM {{ ref('customers_snapshot') }} snap

    {% if is_incremental() %}

    WHERE snap._airbyte_extracted_at >= (
        SELECT COALESCE(MAX(existing._airbyte_extracted_at), '1900-01-01'::timestamp)
        FROM {{ this }} existing
    )

    {% endif %}

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['s.customer_id', 's.dbt_valid_from']) }} AS customer_key,
        s.customer_unique_id,
        s.customer_id,
        s.customer_zip_code_prefix,
        s.customer_city,
        s.customer_state,
        s._airbyte_extracted_at,
        s.update_at,
        s.dbt_valid_from,
        COALESCE(s.dbt_valid_to, '9999-12-31'::timestamp) AS dbt_valid_to,
        CASE
            WHEN s.dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM {{ ref('customers_snapshot') }} s

    {% if is_incremental() %}

    INNER JOIN changed_customers c
        ON s.customer_id = c.customer_id

    {% endif %}

)

SELECT *
FROM final