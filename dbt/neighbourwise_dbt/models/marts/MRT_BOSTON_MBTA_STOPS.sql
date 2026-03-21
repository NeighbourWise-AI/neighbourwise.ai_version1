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

unique_stops AS (
    SELECT
        STOP_ID,
        STOP_NAME,
        MUNICIPALITY,
        LATITUDE,
        LONGITUDE,
        COUNT(DISTINCT ROUTE_ID) AS route_count,
        LISTAGG(DISTINCT ROUTE_NAME, ', ')
            WITHIN GROUP (ORDER BY ROUTE_NAME) AS route_names,
        MAX(CASE WHEN ROUTE_TYPE = 'Heavy Rail (Subway)'   THEN 1 ELSE 0 END) AS serves_heavy_rail,
        MAX(CASE WHEN ROUTE_TYPE = 'Light Rail'            THEN 1 ELSE 0 END) AS serves_light_rail,
        MAX(CASE WHEN ROUTE_TYPE = 'Commuter Rail'         THEN 1 ELSE 0 END) AS serves_commuter_rail,
        MAX(CASE WHEN ROUTE_TYPE = 'Bus'                   THEN 1 ELSE 0 END) AS serves_bus,
        MAX(CASE WHEN ROUTE_TYPE = 'Ferry'                 THEN 1 ELSE 0 END) AS serves_ferry,
        MAX(CASE WHEN WHEELCHAIR_ACCESSIBLE = 1            THEN 1 ELSE 0 END) AS is_wheelchair_accessible
    FROM int_mbta
    WHERE LATITUDE  BETWEEN 42.20 AND 42.55
      AND LONGITUDE BETWEEN -71.35 AND -70.85
    GROUP BY STOP_ID, STOP_NAME, MUNICIPALITY, LATITUDE, LONGITUDE
),

/*
    Spatial join: assign each stop to a MASTER_LOCATION polygon.

    Known boundary issues corrected via stop-level overrides BEFORE the spatial join:
      - place-kencl  (KENMORE):           sits on Fenway/Back Bay boundary → assign to BACK BAY (11)
      - place-bucen  (BU CENTRAL):        falls inside Fenway polygon but is physically in BRIGHTON (18)
      - place-longw  (LONGWOOD station):  falls inside Brookline polygon but serves LONGWOOD (4)
      - 933 (COMMONWEALTH AVE @ BABCOCK): polygon straddles Allston/Brookline → keep ALLSTON (25),
                                          the Brookline duplicate is removed via DISTINCT on stop_id below
*/
with_neighborhood AS (
    SELECT
        s.STOP_ID,
        s.STOP_NAME,
        s.MUNICIPALITY,
        s.LATITUDE,
        s.LONGITUDE,
        s.route_count,
        s.route_names,
        s.serves_heavy_rail,
        s.serves_light_rail,
        s.serves_commuter_rail,
        s.serves_bus,
        s.serves_ferry,
        s.is_wheelchair_accessible,

        -- Apply stop-level location overrides before falling back to spatial join
        CASE
            WHEN s.STOP_ID = 'place-kencl' THEN 11   -- Kenmore → Back Bay
            WHEN s.STOP_ID = 'place-bucen' THEN 18   -- BU Central → Brighton
            WHEN s.STOP_ID = 'place-longw' THEN 4    -- Longwood station → Longwood
            ELSE ml.location_id
        END                                                             AS location_id,

        CASE
            WHEN s.STOP_ID = 'place-kencl' THEN 'BACK BAY'
            WHEN s.STOP_ID = 'place-bucen' THEN 'BRIGHTON'
            WHEN s.STOP_ID = 'place-longw' THEN 'LONGWOOD'
            ELSE COALESCE(ml.neighborhood_name, UPPER(TRIM(s.MUNICIPALITY)))
        END                                                             AS neighborhood_name,

        CASE
            WHEN s.STOP_ID IN ('place-kencl', 'place-bucen', 'place-longw') THEN 'BOSTON'
            ELSE COALESCE(ml.city, UPPER(TRIM(s.MUNICIPALITY)))
        END                                                             AS city,

        CASE
            WHEN s.STOP_ID IN ('place-kencl', 'place-bucen', 'place-longw') THEN 'OVERRIDE'
            WHEN ml.neighborhood_name IS NOT NULL THEN 'SPATIAL'
            ELSE 'MUNICIPALITY'
        END                                                             AS resolution_method

    FROM unique_stops s
    LEFT JOIN master_location ml
        ON ST_CONTAINS(
            ml.geometry,
            ST_MAKEPOINT(s.LONGITUDE, s.LATITUDE)
        )
),

in_coverage AS (
    SELECT w.*
    FROM with_neighborhood w
    WHERE resolution_method IN ('SPATIAL', 'OVERRIDE')
       OR UPPER(TRIM(w.MUNICIPALITY)) IN (
            SELECT DISTINCT UPPER(TRIM(city))
            FROM master_location
        )
),

/*
    Deduplicate stops that fall on polygon boundaries and get assigned
    to multiple neighborhoods. For each stop_id, keep the row with the
    lowest location_id (stable, deterministic tiebreak).
    This handles cases like stop 933 (Commonwealth Ave @ Babcock St)
    which straddles Allston (25) and Brookline (42) — Allston wins.
*/
deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY stop_id
                ORDER BY location_id ASC
            ) AS rn
        FROM in_coverage
    )
    WHERE rn = 1
),

with_description AS (
    SELECT
        STOP_ID                                                         AS stop_id,
        UPPER(TRIM(STOP_NAME))                                          AS stop_name,
        UPPER(TRIM(MUNICIPALITY))                                       AS municipality,
        LATITUDE                                                        AS lat,
        LONGITUDE                                                       AS long,
        route_count,
        route_names,
        serves_heavy_rail::BOOLEAN                                      AS serves_heavy_rail,
        serves_light_rail::BOOLEAN                                      AS serves_light_rail,
        serves_commuter_rail::BOOLEAN                                   AS serves_commuter_rail,
        serves_bus::BOOLEAN                                             AS serves_bus,
        serves_ferry::BOOLEAN                                           AS serves_ferry,
        is_wheelchair_accessible::BOOLEAN                               AS is_wheelchair_accessible,
        location_id,
        neighborhood_name,
        city,
        resolution_method,

        -- Derive transit tier for this stop
        CASE
            WHEN serves_heavy_rail = 1 OR serves_light_rail = 1 THEN 'RAPID TRANSIT'
            WHEN serves_commuter_rail = 1                        THEN 'COMMUTER RAIL'
            WHEN serves_ferry = 1                                THEN 'FERRY'
            WHEN serves_bus = 1                                  THEN 'BUS'
            ELSE 'UNKNOWN'
        END                                                             AS transit_tier,

        -- Row description for GenAI
        UPPER(TRIM(STOP_NAME)) || ' is a ' ||
        CASE
            WHEN serves_heavy_rail = 1 OR serves_light_rail = 1
                THEN 'rapid transit station'
            WHEN serves_commuter_rail = 1
                THEN 'commuter rail station'
            WHEN serves_ferry = 1
                THEN 'ferry terminal'
            ELSE 'bus stop'
        END ||
        ' located in ' || neighborhood_name || ', ' || city || '.' ||
        ' It is served by ' || route_count || ' route(s): ' || route_names || '.' ||
        CASE
            WHEN is_wheelchair_accessible = 1
                THEN ' This stop is wheelchair accessible.'
            ELSE ' This stop is not fully wheelchair accessible.'
        END                                                             AS row_description,

        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')            AS load_timestamp

    FROM deduplicated
)

SELECT *
FROM with_description
WHERE stop_id IS NOT NULL