-- MRT_NEIGHBORHOOD_BLUEBIKES
-- One row per neighborhood summarizing bike share coverage.
-- Score: weighted 0-100 based on station count, dock density, and capacity tier mix.
-- Grades: EXCELLENT(>=75), GOOD(>=50), MODERATE(>=25), LIMITED(<25)
-- Source: MRT_BOSTON_BLUEBIKE_STATIONS + MASTER_LOCATION

{{ config(materialized='table', schema='marts') }}

WITH station_agg AS (
    SELECT
        ml.location_id,
        ml.neighborhood_name,
        ml.city,
        ml.sqmiles,

        COUNT(b.station_number)                                          AS total_stations,
        SUM(CASE WHEN b.capacity_tier = 'LARGE'  THEN 1 ELSE 0 END)     AS large_stations,
        SUM(CASE WHEN b.capacity_tier = 'MEDIUM' THEN 1 ELSE 0 END)     AS medium_stations,
        SUM(CASE WHEN b.capacity_tier = 'SMALL'  THEN 1 ELSE 0 END)     AS small_stations,

        SUM(COALESCE(b.total_docks, 0))                                  AS total_docks,
        CASE
            WHEN COUNT(b.station_number) > 0
            THEN ROUND(SUM(COALESCE(b.total_docks, 0)) * 1.0 / COUNT(b.station_number), 1)
            ELSE 0
        END                                                              AS avg_docks_per_station,

        CASE
            WHEN ml.sqmiles > 0
            THEN ROUND(COUNT(b.station_number) * 1.0 / ml.sqmiles, 2)
            ELSE 0
        END                                                              AS stations_per_sqmile

    FROM {{ ref('MASTER_LOCATION') }} ml
    LEFT JOIN {{ ref('MRT_BOSTON_BLUEBIKE_STATIONS') }} b
        ON b.location_id = ml.location_id
    GROUP BY
        ml.location_id,
        ml.neighborhood_name,
        ml.city,
        ml.sqmiles
),

scored AS (
    SELECT
        *,

        LEAST(
            CASE WHEN sqmiles > 0
                 THEN ROUND(stations_per_sqmile / 5.0 * 40, 1)
                 ELSE 0
            END,
            40
        )                                                                AS density_score,

        LEAST(ROUND(total_docks / 200.0 * 30, 1), 30)                   AS dock_score,

        LEAST(ROUND(total_stations / 20.0 * 20, 1), 20)                 AS station_count_score,

        CASE WHEN large_stations > 0 THEN 10 ELSE 0 END                 AS large_station_score

    FROM station_agg
),

final_scored AS (
    SELECT
        *,

        LEAST(
            ROUND(density_score + dock_score + station_count_score + large_station_score, 1),
            100
        )                                                                AS bikeshare_score,

        CASE
            WHEN LEAST(ROUND(density_score + dock_score + station_count_score + large_station_score, 1), 100) >= 75
                THEN 'EXCELLENT'
            WHEN LEAST(ROUND(density_score + dock_score + station_count_score + large_station_score, 1), 100) >= 50
                THEN 'GOOD'
            WHEN LEAST(ROUND(density_score + dock_score + station_count_score + large_station_score, 1), 100) >= 25
                THEN 'MODERATE'
            ELSE 'LIMITED'
        END                                                              AS bikeshare_grade,

        neighborhood_name || ' (' || city || ') has ' ||
        total_stations || ' Bluebikes stations with ' ||
        total_docks || ' total docks. ' ||
        'Breakdown: ' || large_stations || ' large, ' ||
        medium_stations || ' medium, ' ||
        small_stations || ' small. ' ||
        'Density: ' ||
        CASE WHEN sqmiles > 0
             THEN stations_per_sqmile::VARCHAR
             ELSE '0'
        END || ' stations/sqmile. ' ||
        'Avg docks per station: ' || avg_docks_per_station::VARCHAR      AS row_description

    FROM scored
),

cortex_desc AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        sqmiles,
        total_stations,
        large_stations,
        medium_stations,
        small_stations,
        total_docks,
        avg_docks_per_station,
        stations_per_sqmile,
        density_score,
        dock_score,
        station_count_score,
        large_station_score,
        bikeshare_score,
        bikeshare_grade,
        row_description,

        -- FIX: mistral-7b → mistral-large
        -- FIX: Added Massachusetts grounding + IMPORTANT instruction
        -- FIX: Added Boston/Cambridge area framing to prevent hallucinations
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'You are a neighborhood analyst writing for a Boston/Cambridge, Massachusetts neighborhood guide. '
            || 'In 2-3 sentences, describe the Bluebikes bike share coverage for '
            || neighborhood_name || ', located in ' || city || ', Massachusetts. '
            || 'IMPORTANT: ' || neighborhood_name
            || ' is a neighborhood or city in the Boston/Cambridge, Massachusetts area. '
            || 'Do not confuse it with any place of the same name elsewhere. '
            || 'Do not invent or assume any data not provided below. '
            || 'It has ' || total_stations || ' stations and ' || total_docks || ' total docks. '
            || 'Station breakdown: ' || large_stations || ' large (>=23 docks), '
            || medium_stations || ' medium (15-22 docks), '
            || small_stations || ' small (<15 docks). '
            || 'Stations per square mile: ' || stations_per_sqmile::VARCHAR || '. '
            || 'Bikeshare score: ' || bikeshare_score::VARCHAR || '/100 (' || bikeshare_grade || '). '
            || 'Be factual and concise.'
        )                                                                AS bikeshare_description

    FROM final_scored
)

SELECT
    location_id,
    neighborhood_name,
    city,
    sqmiles,
    total_stations,
    large_stations,
    medium_stations,
    small_stations,
    total_docks,
    avg_docks_per_station,
    stations_per_sqmile,
    bikeshare_score,
    bikeshare_grade,
    bikeshare_description,
    row_description,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM cortex_desc