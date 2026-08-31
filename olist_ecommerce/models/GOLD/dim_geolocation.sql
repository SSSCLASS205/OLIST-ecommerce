{{
  config(
    materialized = 'table',
    )
}}

WITH winning_match AS (
    SELECT
        geolocation_zip_code_prefix,
        standardized_city,
        standardized_state_code,
        full_state_name
    FROM {{ ref('geolocation_silver') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY geolocation_zip_code_prefix
        ORDER BY match_score DESC NULLS LAST
    ) = 1
),

aggregated_data AS (
    SELECT
        geolocation_zip_code_prefix,
        ARRAY_AGG(OBJECT_CONSTRUCT('lat', geolocation_lat, 'lng', geolocation_lng)) AS all_coordinates_in_prefix,
        AVG(geolocation_lat) AS centroid_lat,
        AVG(geolocation_lng) AS centroid_lng
    FROM {{ ref('geolocation_silver') }}
    GROUP BY geolocation_zip_code_prefix
)

SELECT
    a.geolocation_zip_code_prefix,
    w.standardized_city,
    w.standardized_state_code,
    w.full_state_name,
    a.all_coordinates_in_prefix,
    a.centroid_lat,
    a.centroid_lng
FROM aggregated_data a
LEFT JOIN winning_match w
    ON a.geolocation_zip_code_prefix = w.geolocation_zip_code_prefix