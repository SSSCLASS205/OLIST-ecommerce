{{
    config(
        materialized='table'    
    )
}}

WITH bronze_geolocation AS (
    SELECT 
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,
        _airbyte_emitted_at
    FROM {{ source('BRONZE', 'geolocation_bronze') }}
),

official_cities AS (
    SELECT * FROM {{ ref('list_braziliancities') }}
),

city_state_normalization AS (
    SELECT 
        a.geolocation_zip_code_prefix,
        LOWER(b.City) AS standardized_city, 
        UPPER(b.UF) AS standardized_state_code,
        b.State AS full_state_name,
        a.geolocation_lat,
        a.geolocation_lng,
        JAROWINKLER_SIMILARITY(LOWER(a.geolocation_city), LOWER(b.City)) AS match_score
    FROM bronze_geolocation a 
    INNER JOIN official_cities b 
        ON UPPER(a.geolocation_state) = UPPER(b.UF)
        AND JAROWINKLER_SIMILARITY(LOWER(a.geolocation_city), LOWER(b.City)) >= 80
        
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.geolocation_zip_code_prefix, a.geolocation_lat, a.geolocation_lng 
        ORDER BY match_score DESC
    ) = 1
),

aggregated_data AS (
    SELECT 
        geolocation_zip_code_prefix,
        standardized_city,
        standardized_state_code,
        full_state_name,
        ARRAY_AGG(OBJECT_CONSTRUCT('lat', geolocation_lat, 'lng', geolocation_lng)) AS all_coordinates_in_prefix,
        AVG(geolocation_lat) AS centroid_lat,
        AVG(geolocation_lng) AS centroid_lng
    FROM city_state_normalization
    GROUP BY 
        geolocation_zip_code_prefix, 
        standardized_city, 
        standardized_state_code, 
        full_state_name
)

SELECT * FROM aggregated_data