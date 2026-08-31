{{
  config(
    materialized = 'table',
    )
}}

WITH aggregated_data AS (
    SELECT 
        geolocation_zip_code_prefix,
        standardized_city,
        standardized_state_code,
        full_state_name,
        ARRAY_AGG(OBJECT_CONSTRUCT('lat', geolocation_lat, 'lng', geolocation_lng)) AS all_coordinates_in_prefix,
        AVG(geolocation_lat) AS centroid_lat,
        AVG(geolocation_lng) AS centroid_lng
    FROM {{ref("geolocation_silver")}}
    GROUP BY 
        geolocation_zip_code_prefix, 
        standardized_city, 
        standardized_state_code, 
        full_state_name
)

select  * FROM aggregated_data

