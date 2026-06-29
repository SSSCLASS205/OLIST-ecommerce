{% snapshot sellers_snapshot %}


{{
    config(
        target_database="OLIST_WAREHOUSE",
        target_schema="SILVER",
        unique_key='seller_id',
        strategy='check',
        check_cols=['seller_id', 'seller_state', 'seller_zip_code_prefix']
    )
}}

SELECT * FROM {{ source('BRONZE', 'sellers_bronze') }}

{% endsnapshot %}