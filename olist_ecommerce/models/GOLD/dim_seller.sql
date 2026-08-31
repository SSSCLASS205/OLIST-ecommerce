{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='seller_key'
    )
}}

WITH changed_sellers AS (

    SELECT DISTINCT
        seller_id
    FROM {{ ref('sellers_snapshot') }}

    {% if is_incremental() %}

    WHERE _airbyte_extracted_at >= (
        SELECT COALESCE(
            MAX(_airbyte_extracted_at),
            '1900-01-01'::timestamp
        )
        FROM {{ this }}
    )

    {% endif %}
),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['a.seller_id', 'a.dbt_valid_from']) }} AS seller_key,
        a.seller_id,
        a.seller_zip_code_prefix,
        a.seller_city,
        a.seller_state,
        a._airbyte_extracted_at,
        a.update_at,
        a.dbt_valid_from,
        COALESCE(a.dbt_valid_to, '9999-12-31'::timestamp) AS dbt_valid_to,
        CASE
            WHEN a.dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM {{ ref('sellers_snapshot') }} a
    
    {% if is_incremental() %}

    INNER JOIN changed_sellers b
        ON a.seller_id = b.seller_id
    
    {% endif %}

)

SELECT *
FROM final