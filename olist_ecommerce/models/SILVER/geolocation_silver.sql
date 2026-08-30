{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='geolocation_id'
    )
}}

WITH watermark AS (
    {% if is_incremental() %}
    SELECT MAX(_airbyte_emitted_at) AS max_emitted_at FROM {{ this }}
    {% else %}
    SELECT NULL::timestamp AS max_emitted_at
    {% endif %}
),

bronze_geolocation AS (
    SELECT
        g.geolocation_id,
        g.geolocation_zip_code_prefix,
        g.geolocation_lat,
        g.geolocation_lng,
        g.geolocation_city,
        g.geolocation_state,
        g._airbyte_emitted_at
    FROM {{ source('BRONZE', 'geolocation_bronze') }} g
    CROSS JOIN watermark w
    WHERE LENGTH(TRIM(g.geolocation_zip_code_prefix)) > 0
    {% if is_incremental() %}
    AND g._airbyte_emitted_at >= w.max_emitted_at
    {% endif %}
),

official_cities AS (
    SELECT * FROM {{ ref('list_braziliancities') }}
),

city_state_normalization AS (
    SELECT 
        a.geolocation_id,
        a.geolocation_zip_code_prefix,
        LOWER(b.City) AS standardized_city, 
        UPPER(b.UF) AS standardized_state_code,
        b.State AS full_state_name,
        a.geolocation_lat,
        a.geolocation_lng,
        JAROWINKLER_SIMILARITY(LOWER(a.geolocation_city), LOWER(b.City)) AS match_score
    FROM bronze_geolocation a 
    LEFT JOIN official_cities b 
        ON UPPER(a.geolocation_state) = UPPER(b.UF)
        AND JAROWINKLER_SIMILARITY(LOWER(a.geolocation_city), LOWER(b.City)) >= 80
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.geolocation_id 
        ORDER BY match_score DESC
    ) = 1
)

SELECT * FROM city_state_normalization