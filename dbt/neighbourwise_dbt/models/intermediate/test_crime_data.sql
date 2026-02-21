-- models/intermediate/test_crime_data.sql

{{ config(
    materialized='table',
) }}

SELECT 
    *
FROM {{ source('stage', 'stg_boston_crime') }}
LIMIT 100