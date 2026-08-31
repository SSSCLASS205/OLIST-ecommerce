{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_predicates=[
        "DBT_INTERNAL_DEST.update_at < DBT_INTERNAL_SOURCE.update_at"
    ]
) }}

WITH bronze_products AS (
    SELECT 
        product_id,
        product_category_name,
        product_name_lenght AS product_name_length,
        product_description_lenght AS product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        update_at,
        _airbyte_extracted_at
    FROM {{ source('BRONZE', 'products_bronze') }}
    {% if is_incremental() %}
    WHERE _airbyte_extracted_at > (SELECT MAX(_airbyte_extracted_at) FROM {{ this }})
    {% endif %}
),

category_translation AS (
    SELECT 
        product_category_name,
        product_category_name_english
    FROM {{ source('BRONZE', 'product_category_name_translation_bronze') }}
),

ranked_products AS (
    SELECT 
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY p.product_id 
            ORDER BY p.update_at DESC, p._airbyte_extracted_at DESC
        ) AS rnk
    FROM bronze_products p
),

deduped_products AS (
    SELECT * FROM ranked_products WHERE rnk = 1
)

SELECT 
    bp.product_id,
    COALESCE(t.product_category_name_english, bp.product_category_name) AS product_category_name,
    bp.product_name_length,
    bp.product_description_length,
    bp.product_photos_qty,
    bp.product_weight_g,
    bp.product_length_cm,
    bp.product_height_cm,
    bp.product_width_cm,
    bp.update_at,
    bp._airbyte_extracted_at
FROM deduped_products bp
LEFT JOIN category_translation t 
    ON bp.product_category_name = t.product_category_name