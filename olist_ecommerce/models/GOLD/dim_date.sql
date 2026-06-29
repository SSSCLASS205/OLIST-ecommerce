{{ config(
    materialized='table'
) }}


with raw_numbers as (
    SELECT ROW_NUMBER() over() -1 AS row_num
    FROM TABLE(GENERATOR(ROWCOUNT => 7305))
),
date_spine AS(
    SELECT 
        DATEADD(day, row_num, '2015-01-01'::DATE) AS date_day
    FROM raw_numbers
),
final_date_dimension AS (
    SELECT
        date_day AS date_actual,
        REPLACE(date_day::VARCHAR, '-', '')::INT AS date_id, 
        
        EXTRACT(year FROM date_day) AS year_number,
        'CY ' || EXTRACT(year FROM date_day) AS year_code,
        
        EXTRACT(quarter FROM date_day) AS quarter_number,
        'Q' || EXTRACT(quarter FROM date_day) AS quarter_code,
        EXTRACT(year FROM date_day) || '-Q' || EXTRACT(quarter FROM date_day) AS year_quarter_code,
        
        EXTRACT(month FROM date_day) AS month_number,
        TO_CHAR(date_day, 'MMMM') AS month_name,
        TO_CHAR(date_day, 'MON') AS month_name_short,
        EXTRACT(year FROM date_day) || '-' || TO_CHAR(date_day, 'MM') AS year_month_code,
        
        EXTRACT(week FROM date_day) AS week_of_year,
        TRUNC(date_day, 'week')::DATE AS week_start_date,
        
        EXTRACT(day FROM date_day) AS day_of_month,
        EXTRACT(dayofweek FROM date_day) AS day_of_week_number,
        TO_CHAR(date_day, 'DAY') AS day_name,
        TO_CHAR(date_day, 'DY') AS day_name_short,
        
        CASE 
            WHEN EXTRACT(dayofweek FROM date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend
    FROM date_spine
)

SELECT * FROM final_date_dimension

