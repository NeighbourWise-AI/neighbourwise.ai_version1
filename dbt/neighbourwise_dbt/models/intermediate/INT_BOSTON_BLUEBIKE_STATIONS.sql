{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('stage', 'stg_boston_bluebikes_stations') }}
),

cleaned AS (
    SELECT
        CAST(NUMBER AS VARCHAR(20)) AS station_number
        , COALESCE(NULLIF(TRIM(UPPER(NAME)), ''), 'UNKNOWN') AS station_name
        , COALESCE(
            CASE
                WHEN UPPER(TRIM(DISTRICT)) IN (
                    'ARLINGTON', 'BOSTON', 'BROOKLINE', 'CAMBRIDGE',
                    'CHELSEA', 'EVERETT', 'MALDEN', 'MEDFORD',
                    'NEWTON', 'REVERE', 'SALEM', 'SOMERVILLE', 'WATERTOWN'
                ) THEN UPPER(TRIM(DISTRICT))
                ELSE NULL
            END,
            'UNKNOWN'
        ) AS district
        , COALESCE(
            CASE
                WHEN LATITUDE  BETWEEN 42.20 AND 42.55
                 AND LONGITUDE BETWEEN -71.35 AND -70.85
                THEN LATITUDE
                ELSE NULL
            END,
            -999.0
        ) AS lat
        , COALESCE(
            CASE
                WHEN LATITUDE  BETWEEN 42.20 AND 42.55
                 AND LONGITUDE BETWEEN -71.35 AND -70.85
                THEN LONGITUDE
                ELSE NULL
            END,
            -999.0
        ) AS long
        , CASE
            WHEN LATITUDE  BETWEEN 42.20 AND 42.55
             AND LONGITUDE BETWEEN -71.35 AND -70.85
            THEN TRUE
            ELSE FALSE
        END AS has_valid_location
        , COALESCE(CAST(TOTAL_DOCKS AS INTEGER), -99) AS total_docks
        , CASE
            WHEN CAST(TOTAL_DOCKS AS INTEGER) >= 23 THEN 'LARGE'
            WHEN CAST(TOTAL_DOCKS AS INTEGER) BETWEEN 15 AND 22 THEN 'MEDIUM'
            WHEN CAST(TOTAL_DOCKS AS INTEGER) BETWEEN 1 AND 14 THEN 'SMALL'
            ELSE 'UNKNOWN'
        END AS capacity_tier
        , TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp
    FROM source
)

SELECT *
FROM cleaned
WHERE station_number IS NOT NULL