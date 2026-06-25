{{
  config(
    materialized = 'table',
    )
}}
select  * FROM {{ref("geolocation_silver")}}

