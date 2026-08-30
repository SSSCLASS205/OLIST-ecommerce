{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='geolocation_id',
        tmp_relation_type='table'

    )
}}

WITH bronze_geolocation AS (
    SELECT
        geolocation_id,
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,
        _airbyte_emitted_at
    FROM {{ source('BRONZE', 'geolocation_bronze') }}
    WHERE LENGTH(TRIM(geolocation_zip_code_prefix)) > 0 
    {% if is_incremental() %}
        AND  _airbyte_emitted_at >= (SELECT MAX(_airbyte_emitted_at) FROM {{this}})
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