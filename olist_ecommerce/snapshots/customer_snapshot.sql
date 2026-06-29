{% snapshot customers_snapshot %}

{{
    config(
        target_database='OLIST_WAREHOUSE',
        target_schema=target.schema ~ '_SILVER' if target.name != 'prod' else 'SILVER',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_city', 'customer_state', 'customer_zip_code_prefix']
    )
}}

SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _airbyte_emitted_at
FROM {{ source('BRONZE', 'customers_bronze') }}

{% endsnapshot %}