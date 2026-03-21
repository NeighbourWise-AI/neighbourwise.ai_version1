{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('stage', 'stg_master_location') }}
),

cleaned AS (
    SELECT
        CAST(LOCATION_ID AS INTEGER) AS location_id
        , COALESCE(NULLIF(TRIM(UPPER(NAME)), ''), 'UNKNOWN') AS neighborhood_name
        , COALESCE(NULLIF(TRIM(UPPER(CITY)), ''), 'UNKNOWN') AS city
        , COALESCE(NULLIF(TRIM(UPPER(STATE)), ''), 'UNKNOWN') AS state
        , COALESCE(NULLIF(TRIM(UPPER(GRANULARITY)), ''), 'UNKNOWN') AS granularity
        , CASE
            WHEN GEOMETRY_WKT IS NOT NULL
            THEN ST_GEOGRAPHYFROMWKT(GEOMETRY_WKT)
            ELSE NULL
        END AS geometry
        , CASE
            WHEN GEOMETRY_WKT IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_geometry
        , COALESCE(CAST(CENTROID_LAT AS FLOAT), -999.0) AS centroid_lat
        , COALESCE(CAST(CENTROID_LONG AS FLOAT), -999.0) AS centroid_long
        , CASE
            WHEN CAST(CENTROID_LAT AS FLOAT)  BETWEEN 42.20 AND 42.55
             AND CAST(CENTROID_LONG AS FLOAT) BETWEEN -71.35 AND -70.85
            THEN TRUE
            ELSE FALSE
        END AS has_valid_location
        , COALESCE(CAST(SQMILES AS FLOAT), -99.0) AS sqmiles
        , CASE
            WHEN UPPER(TRIM(CITY)) = 'BOSTON' THEN TRUE
            ELSE FALSE
        END AS is_boston
        , CASE
            WHEN UPPER(TRIM(CITY)) = 'CAMBRIDGE' THEN TRUE
            ELSE FALSE
        END AS is_cambridge
        , CASE
            WHEN UPPER(TRIM(CITY)) NOT IN ('BOSTON', 'CAMBRIDGE') THEN TRUE
            ELSE FALSE
        END AS is_greater_boston
        , SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-7b',
            'In 2-3 sentences, describe the neighborhood or city called ' || UPPER(TRIM(NAME)) ||
            ' located in ' || UPPER(TRIM(CITY)) || ', Massachusetts. ' ||
            'Focus on its geographic character, what it is known for, and what type of ' ||
            'residents or lifestyle it suits. Be factual and concise. Do not use bullet points.'
        ) AS location_description
        , TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp
    FROM source
)

SELECT *
FROM cleaned
WHERE location_id IS NOT NULL
ORDER BY is_boston DESC, is_cambridge DESC, neighborhood_name ASC