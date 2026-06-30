{{ 
    config(materialized='table')
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['seller_id', 'dbt_valid_from']) }} AS customer_pk,
    seller_id,
    seller_zip_code_prefix,
    dbt_valid_from,
    COALESCE(dbt_valid_to, '9999-12-31'::timestamp) AS dbt_valid_to
FROM {{ref('sellers_snapshot')}}