{% snapshot sellers_snapshot %}

{{
    config(
        target_database='OLIST_WAREHOUSE',
        target_schema= (target.schema ~ '_SILVER') if target.name != 'prod' else 'SILVER',
        unique_key='seller_id',
        strategy='timestamp',
        updated_at='update_at'
    )
}}

WITH dedup_seller AS (
    SELECT 
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        _airbyte_extracted_at,
        update_at,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id 
            ORDER BY update_at DESC, _airbyte_extracted_at DESC
        ) AS rnk 
    FROM {{ source('BRONZE', 'sellers_bronze') }}
),

final_dedup AS (
    SELECT 
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        _airbyte_extracted_at,
        update_at
    FROM dedup_seller
    WHERE rnk = 1
)

SELECT 
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    _airbyte_extracted_at,
    update_at
FROM final_dedup

{% endsnapshot %}