{{ config(
    materialized='incremental',
    unique_key='review_id',
    incremental_predicates=[
        "DBT_INTERNAL_DEST.update_at < DBT_INTERNAL_SOURCE.update_at"
    ],
    incremental_strategy='merge'
) }}

WITH bronze_reviews AS (
    SELECT 
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        _airbyte_emitted_at,
        update_at
    FROM {{ source('BRONZE', 'order_reviews_bronze') }}
    
    {% if is_incremental() %}
        WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

deduped_reviews AS (
    SELECT 
        *
    FROM bronze_reviews
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY review_id
        ORDER BY update_at DESC ,
        _airbyte_emitted_at DESC) = 1
)

SELECT 
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    _airbyte_emitted_at,
    update_at
FROM deduped_reviews