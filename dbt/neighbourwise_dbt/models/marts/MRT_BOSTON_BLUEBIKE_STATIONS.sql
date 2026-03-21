{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH int_bluebikes AS (
    SELECT * FROM {{ ref('INT_BOSTON_BLUEBIKE_STATIONS') }}
),

master_location AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        geometry
    FROM {{ ref('MASTER_LOCATION') }}
    WHERE has_geometry = TRUE
),

with_neighborhood AS (
    SELECT
        b.*,
        ml.location_id AS location_id,
        COALESCE(ml.neighborhood_name, b.district) AS neighborhood_name,
        COALESCE(ml.city, b.district) AS city,
        CASE
            WHEN ml.neighborhood_name IS NOT NULL THEN 'SPATIAL'
            ELSE 'DISTRICT'
        END AS resolution_method
    FROM int_bluebikes b
    LEFT JOIN master_location ml
        ON b.has_valid_location = TRUE
        AND ST_CONTAINS(
            ml.geometry,
            ST_MAKEPOINT(
                CASE WHEN b.has_valid_location = TRUE THEN b.long ELSE 0 END,
                CASE WHEN b.has_valid_location = TRUE THEN b.lat  ELSE 0 END
            )
        )
),

with_description AS (
    SELECT
        station_number,
        station_name,
        district,
        lat,
        long,
        has_valid_location,
        total_docks,
        capacity_tier,
        location_id,
        neighborhood_name,
        city,
        resolution_method,
        station_name || ' is a ' ||
        LOWER(capacity_tier) || ' capacity Blue Bikes station located in ' ||
        neighborhood_name || ', ' || city || '.' ||
        ' It has ' || CAST(total_docks AS VARCHAR) || ' total docks.' ||
        CASE
            WHEN capacity_tier = 'LARGE'  THEN ' This is a high-capacity station suitable for busy areas.'
            WHEN capacity_tier = 'MEDIUM' THEN ' This is a standard capacity station.'
            WHEN capacity_tier = 'SMALL'  THEN ' This is a small station with limited availability.'
            ELSE ''
        END AS row_description,
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp

    FROM with_neighborhood
)

SELECT * FROM with_description
WHERE station_number IS NOT NULL