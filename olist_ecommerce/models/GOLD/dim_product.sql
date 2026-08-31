{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_predicates=[
        "DBT_INTERNAL_DEST.update_at < DBT_INTERNAL_SOURCE.update_at"
    ]
) }}


SELECT * 
FROM {{ref("product_silver")}}
{%if is_incremental()%}
    WHERE _airbyte_extracted_at >= (SELECT MAX(_airbyte_extracted_at) FROM {{this}}) 
{%endif%}