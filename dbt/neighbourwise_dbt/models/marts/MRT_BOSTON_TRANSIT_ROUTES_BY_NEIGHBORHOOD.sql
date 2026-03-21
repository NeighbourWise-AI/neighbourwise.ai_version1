{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH int_mbta AS (
    SELECT * FROM {{ source('intermediate', 'int_boston_mbta_mapping') }}
),

master_location AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        geometry,
        is_boston,
        is_cambridge,
        is_greater_boston
    FROM {{ ref('MASTER_LOCATION') }}
    WHERE has_geometry = TRUE
),

-- Filter to bounding box first (cheap filter before spatial join)
in_bounds AS (
    SELECT *
    FROM int_mbta
    WHERE LATITUDE  BETWEEN 42.20 AND 42.55
      AND LONGITUDE BETWEEN -71.35 AND -70.85
),

-- Spatial join each stop-route row to a neighborhood
-- Keep direction = 0 (Outbound) only to avoid duplicating routes per neighborhood
-- Direction is irrelevant for "which routes pass through this neighborhood"
outbound_only AS (
    SELECT *
    FROM in_bounds
    WHERE DIRECTION_ID = 0
),

with_neighborhood AS (
    SELECT
        m.ROUTE_ID,
        m.ROUTE_NAME,
        m.ROUTE_TYPE,
        m.STOP_ID,
        m.STOP_NAME,
        m.STOP_SEQUENCE,
        m.MUNICIPALITY,
        m.LATITUDE,
        m.LONGITUDE,
        m.WHEELCHAIR_ACCESSIBLE,
        ml.location_id,
        COALESCE(ml.neighborhood_name, UPPER(TRIM(m.MUNICIPALITY))) AS neighborhood_name,
        COALESCE(ml.city, UPPER(TRIM(m.MUNICIPALITY)))              AS city,
        CASE
            WHEN ml.neighborhood_name IS NOT NULL THEN 'SPATIAL'
            ELSE 'MUNICIPALITY'
        END AS resolution_method
    FROM outbound_only m
    LEFT JOIN master_location ml
        ON ST_CONTAINS(
            ml.geometry,
            ST_MAKEPOINT(m.LONGITUDE, m.LATITUDE)
        )
),

-- Filter to coverage area only
in_coverage AS (
    SELECT *
    FROM with_neighborhood
    WHERE resolution_method = 'SPATIAL'
       OR UPPER(TRIM(MUNICIPALITY)) IN (
            SELECT DISTINCT UPPER(TRIM(city))
            FROM master_location
        )
),

-- Aggregate to one row per neighborhood + route
-- This is the key grain for route planning queries
neighborhood_routes AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        resolution_method,
        ROUTE_ID                                                        AS route_id,
        ROUTE_NAME                                                      AS route_name,
        ROUTE_TYPE                                                      AS route_type,

        -- How many stops does this route have in this neighborhood?
        COUNT(DISTINCT STOP_ID)                                         AS stop_count_in_neighborhood,
        LISTAGG(STOP_NAME, ' → ')
            WITHIN GROUP (ORDER BY STOP_SEQUENCE)                       AS stops_in_neighborhood,

        -- First and last stop of this route within the neighborhood
        MIN(STOP_SEQUENCE)                                              AS entry_sequence,
        MAX(STOP_SEQUENCE)                                              AS exit_sequence,

        -- Is this route accessible throughout this neighborhood?
        MIN(WHEELCHAIR_ACCESSIBLE)                                      AS min_accessibility,

        -- Transit tier for sorting/scoring
        CASE
            WHEN ROUTE_TYPE = 'Heavy Rail (Subway)'  THEN 1
            WHEN ROUTE_TYPE = 'Light Rail'           THEN 2
            WHEN ROUTE_TYPE = 'Commuter Rail'        THEN 3
            WHEN ROUTE_TYPE = 'Ferry'                THEN 4
            WHEN ROUTE_TYPE = 'Bus'                  THEN 5
            ELSE 6
        END                                                             AS route_tier

    FROM in_coverage
    GROUP BY
        location_id,
        neighborhood_name,
        city,
        resolution_method,
        ROUTE_ID,
        ROUTE_NAME,
        ROUTE_TYPE
),

with_description AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        resolution_method,
        route_id,
        route_name,
        route_type,
        stop_count_in_neighborhood,
        stops_in_neighborhood,
        entry_sequence,
        exit_sequence,
        CASE WHEN min_accessibility = 1 THEN TRUE ELSE FALSE END        AS is_accessible,
        route_tier,

        -- Row description for GenAI route planning queries
        route_name || ' (' || route_type || ') passes through ' ||
        neighborhood_name || ', ' || city || '.' ||
        ' It serves ' || stop_count_in_neighborhood || ' stop(s) here: ' ||
        stops_in_neighborhood || '.' ||
        CASE
            WHEN route_type = 'Heavy Rail (Subway)'
                THEN ' This is a rapid transit subway route providing fast city-wide connectivity.'
            WHEN route_type = 'Light Rail'
                THEN ' This is a light rail (Green Line) route providing surface and underground connectivity.'
            WHEN route_type = 'Commuter Rail'
                THEN ' This is a commuter rail route connecting to the wider Greater Boston region.'
            WHEN route_type = 'Ferry'
                THEN ' This is a ferry route providing water-based transit connectivity.'
            WHEN route_type = 'Bus'
                THEN ' This is a local bus route providing neighborhood-level connectivity.'
            ELSE ''
        END                                                             AS row_description,

        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')            AS load_timestamp

    FROM neighborhood_routes
)

SELECT *
FROM with_description
WHERE route_id IS NOT NULL
ORDER BY neighborhood_name, route_tier, route_id