{% snapshot customers_snapshot %}

{{
    config(
        target_database='OLIST_WAREHOUSE',
        target_schema=(target.schema ~ '_SILVER') if target.name != 'prod' else 'SILVER',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='update_at'
    )
}}

WITH dedup_customer as (

    SELECT 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _airbyte_extracted_at,
        update_at,
        ROW_NUMBER() over(PARTITION BY customer_id 
            ORDER BY update_at DESC ,
                _airbyte_extracted_at DESC
        ) as rnk 
    FROM {{ source('BRONZE', 'customers_bronze') }}
),
final_dedup as (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _airbyte_extracted_at,
        update_at
    FROM dedup_customer
    WHERE rnk = 1
)

SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _airbyte_extracted_at,
    update_at
FROM final_dedup

{% endsnapshot %}