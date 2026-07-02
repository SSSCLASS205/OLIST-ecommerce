{% snapshot sellers_snapshot %}


{{
    config(
        target_database='OLIST_WAREHOUSE',
        target_schema= (target.schema ~ '_SILVER') if target.name != 'prod' else 'SILVER',
        unique_key='seller_id',
        strategy='check',
        check_cols=['seller_id', 'seller_state', 'seller_zip_code_prefix']
    )
}}

SELECT 
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    _airbyte_emitted_at
FROM {{ source('BRONZE', 'sellers_bronze') }}

{% endsnapshot %}
