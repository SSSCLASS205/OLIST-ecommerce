{% snapshot customers_snapshot %}

{{
    config(
        target_database='OLIST_warehouse',
        target_schema='SILVER',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_city', 'customer_state', 'customer_zip_code_prefix']
    )
}}



SELECT * FROM {{ source('BRONZE', 'customers_bronze') }}


{% endsnapshot %}