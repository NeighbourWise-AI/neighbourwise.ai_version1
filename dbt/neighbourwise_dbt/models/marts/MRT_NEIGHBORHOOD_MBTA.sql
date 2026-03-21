{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH routes_by_neighborhood AS (
    SELECT * FROM {{ ref('MRT_BOSTON_TRANSIT_ROUTES_BY_NEIGHBORHOOD') }}
),

stops_by_neighborhood AS (
    SELECT * FROM {{ ref('MRT_BOSTON_MBTA_STOPS') }}
),

-- Aggregate stop-level metrics per neighborhood
stop_metrics AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        COUNT(DISTINCT stop_id)                                         AS total_stops,
        COUNT(DISTINCT CASE
            WHEN serves_heavy_rail OR serves_light_rail
            THEN stop_id END)                                           AS rapid_transit_stops,
        COUNT(DISTINCT CASE
            WHEN serves_commuter_rail
            THEN stop_id END)                                           AS commuter_rail_stops,
        COUNT(DISTINCT CASE
            WHEN serves_bus
            THEN stop_id END)                                           AS bus_stops,
        COUNT(DISTINCT CASE
            WHEN serves_ferry
            THEN stop_id END)                                           AS ferry_stops,
        COUNT(DISTINCT CASE
            WHEN is_wheelchair_accessible
            THEN stop_id END)                                           AS accessible_stops
    FROM stops_by_neighborhood
    GROUP BY location_id, neighborhood_name, city
),

-- Aggregate route-level metrics per neighborhood
route_metrics AS (
    SELECT
        location_id,
        COUNT(DISTINCT route_id)                                        AS total_routes,
        COUNT(DISTINCT CASE
            WHEN route_type IN ('Heavy Rail (Subway)', 'Light Rail')
            THEN route_id END)                                          AS rapid_transit_routes,
        COUNT(DISTINCT CASE
            WHEN route_type = 'Commuter Rail'
            THEN route_id END)                                          AS commuter_rail_routes,
        COUNT(DISTINCT CASE
            WHEN route_type = 'Bus'
            THEN route_id END)                                          AS bus_routes,
        COUNT(DISTINCT CASE
            WHEN route_type = 'Ferry'
            THEN route_id END)                                          AS ferry_routes,
        LISTAGG(DISTINCT CASE
            WHEN route_type IN ('Heavy Rail (Subway)', 'Light Rail')
            THEN route_name END, ', ')                                  AS rapid_transit_route_names,
        LISTAGG(DISTINCT route_name, ', ')                              AS all_route_names
    FROM routes_by_neighborhood
    GROUP BY location_id
),

-- Join stop and route metrics
combined AS (
    SELECT
        s.location_id,
        s.neighborhood_name,
        s.city,
        s.total_stops,
        s.rapid_transit_stops,
        s.commuter_rail_stops,
        s.bus_stops,
        s.ferry_stops,
        s.accessible_stops,
        r.total_routes,
        r.rapid_transit_routes,
        r.commuter_rail_routes,
        r.bus_routes,
        r.ferry_routes,
        COALESCE(NULLIF(TRIM(r.rapid_transit_route_names), ''), 'NO RAPID TRANSIT') AS rapid_transit_route_names,
        COALESCE(NULLIF(TRIM(r.all_route_names), ''), 'NO ROUTES')                 AS all_route_names,

        -- Boolean flags
        (r.rapid_transit_routes > 0)                                    AS has_rapid_transit,
        (r.commuter_rail_routes > 0)                                    AS has_commuter_rail,
        (r.bus_routes > 0)                                              AS has_bus,
        (r.ferry_routes > 0)                                            AS has_ferry,

        -- Wheelchair accessibility percentage
        ROUND(
            CASE WHEN s.total_stops > 0
                THEN (s.accessible_stops * 100.0 / s.total_stops)
                ELSE 0
            END, 1
        )                                                               AS pct_accessible_stops,

        -- Transit score (weighted)
        -- Rapid transit stops = 3pts, Commuter Rail = 2pts, Ferry = 2pts, Bus = 1pt
        -- Bus-only neighborhoods (no rapid transit or commuter rail) capped at 85
        -- All others capped at 100
        LEAST(
            (s.rapid_transit_stops  * 3) +
            (s.commuter_rail_stops  * 2) +
            (s.ferry_stops          * 2) +
            (s.bus_stops            * 1),
            CASE
                WHEN s.rapid_transit_stops = 0 AND s.commuter_rail_stops = 0
                THEN 85
                ELSE 100
            END
        )                                                               AS transit_score,

        -- Transit grade based on score
        CASE
            WHEN LEAST(
                (s.rapid_transit_stops * 3) +
                (s.commuter_rail_stops * 2) +
                (s.ferry_stops         * 2) +
                (s.bus_stops           * 1),
                CASE
                    WHEN s.rapid_transit_stops = 0 AND s.commuter_rail_stops = 0
                    THEN 85
                    ELSE 100
                END
            ) >= 75 THEN 'EXCELLENT'
            WHEN LEAST(
                (s.rapid_transit_stops * 3) +
                (s.commuter_rail_stops * 2) +
                (s.ferry_stops         * 2) +
                (s.bus_stops           * 1),
                CASE
                    WHEN s.rapid_transit_stops = 0 AND s.commuter_rail_stops = 0
                    THEN 85
                    ELSE 100
                END
            ) >= 50 THEN 'GOOD'
            WHEN LEAST(
                (s.rapid_transit_stops * 3) +
                (s.commuter_rail_stops * 2) +
                (s.ferry_stops         * 2) +
                (s.bus_stops           * 1),
                CASE
                    WHEN s.rapid_transit_stops = 0 AND s.commuter_rail_stops = 0
                    THEN 85
                    ELSE 100
                END
            ) >= 25 THEN 'MODERATE'
            ELSE 'LIMITED'
        END                                                             AS transit_grade,

    FROM stop_metrics s
    LEFT JOIN route_metrics r ON s.location_id = r.location_id
),

-- Cortex-generated description
with_description AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        total_stops,
        rapid_transit_stops,
        commuter_rail_stops,
        bus_stops,
        ferry_stops,
        accessible_stops,
        total_routes,
        rapid_transit_routes,
        commuter_rail_routes,
        bus_routes,
        ferry_routes,
        rapid_transit_route_names,
        all_route_names,
        has_rapid_transit,
        has_commuter_rail,
        has_bus,
        has_ferry,
        pct_accessible_stops,
        transit_score,
        transit_grade,
        -- Let Cortex write freely, then fix any wrong city/neighborhood labels in SQL
        TRIM(
            -- Step 2: fix "city of X" → correct label for Boston/Cambridge neighborhoods
            CASE
                WHEN city = 'Boston' THEN
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            SNOWFLAKE.CORTEX.COMPLETE(
                                'mistral-large',
                                'You are a Boston-area transit expert. Write a 2-3 sentence transit summary for ' || neighborhood_name || ' (' || city || '). ' ||
                                'Be specific about transit options and destinations. No bullet points. Plain prose only.' ||
                                CHR(10) ||
                                'Transit score: ' || transit_score || ' out of 100 (' || transit_grade || ')' ||
                                CHR(10) ||
                                'Rapid transit routes: ' || COALESCE(rapid_transit_route_names, 'None') ||
                                CHR(10) ||
                                'All routes: ' || COALESCE(all_route_names, 'None') ||
                                CHR(10) ||
                                'Total stops: ' || total_stops ||
                                CHR(10) ||
                                'Rapid transit stops: ' || rapid_transit_stops ||
                                CHR(10) ||
                                'Bus stops: ' || bus_stops ||
                                CHR(10) ||
                                'Wheelchair accessible stops: ' || pct_accessible_stops || '%'
                            ),
                            -- Replace "the city of X" or "X, a city" with neighborhood phrasing
                            '(?i)(the city of ' || neighborhood_name || '|' || neighborhood_name || ',? a city[^,]*)',
                            'the ' || INITCAP(neighborhood_name) || ' neighborhood of Boston'
                        ),
                        -- Replace bare "city" references near the name
                        '(?i)(city or town of ' || neighborhood_name || ')',
                        'the ' || INITCAP(neighborhood_name) || ' neighborhood of Boston'
                    )
                WHEN city = 'Cambridge' THEN
                    REGEXP_REPLACE(
                        SNOWFLAKE.CORTEX.COMPLETE(
                            'mistral-large',
                            'You are a Boston-area transit expert. Write a 2-3 sentence transit summary for ' || neighborhood_name || ' (' || city || '). ' ||
                            'Be specific about transit options and destinations. No bullet points. Plain prose only.' ||
                            CHR(10) ||
                            'Transit score: ' || transit_score || ' out of 100 (' || transit_grade || ')' ||
                            CHR(10) ||
                            'Rapid transit routes: ' || COALESCE(rapid_transit_route_names, 'None') ||
                            CHR(10) ||
                            'All routes: ' || COALESCE(all_route_names, 'None') ||
                            CHR(10) ||
                            'Total stops: ' || total_stops ||
                            CHR(10) ||
                            'Rapid transit stops: ' || rapid_transit_stops ||
                            CHR(10) ||
                            'Bus stops: ' || bus_stops ||
                            CHR(10) ||
                            'Wheelchair accessible stops: ' || pct_accessible_stops || '%'
                        ),
                        '(?i)(the city of ' || neighborhood_name || '|' || neighborhood_name || ',? a city[^,]*)',
                        'the ' || INITCAP(neighborhood_name) || ' neighborhood of Cambridge'
                    )
                ELSE
                    -- Greater Boston cities: just let Cortex write freely, no replacement needed
                    SNOWFLAKE.CORTEX.COMPLETE(
                        'mistral-large',
                        'You are a Boston-area transit expert. Write a 2-3 sentence transit summary for the city of ' || neighborhood_name || '. ' ||
                        'Be specific about transit options and destinations. No bullet points. Plain prose only.' ||
                        CHR(10) ||
                        'Transit score: ' || transit_score || ' out of 100 (' || transit_grade || ')' ||
                        CHR(10) ||
                        'Rapid transit routes: ' || COALESCE(rapid_transit_route_names, 'None') ||
                        CHR(10) ||
                        'All routes: ' || COALESCE(all_route_names, 'None') ||
                        CHR(10) ||
                        'Total stops: ' || total_stops ||
                        CHR(10) ||
                        'Rapid transit stops: ' || rapid_transit_stops ||
                        CHR(10) ||
                        'Bus stops: ' || bus_stops ||
                        CHR(10) ||
                        'Wheelchair accessible stops: ' || pct_accessible_stops || '%'
                    )
            END
        )                                                               AS transit_description,

        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')            AS load_timestamp

    FROM combined
)

SELECT *
FROM with_description
WHERE location_id IS NOT NULL
ORDER BY transit_score DESC