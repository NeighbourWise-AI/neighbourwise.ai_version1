{{
    config(
        materialized='incremental',
        unique_key='incident_number',
        schema='marts'
    )
}}

WITH int_boston AS (
    SELECT
        incident_number,
        CAST(offense_code AS VARCHAR(20)) AS offense_code,
        offense_description,
        district,
        district_name,
        'N/A'           AS source_neighborhood,
        reporting_area,
        street,
        lat,
        long,
        valid_location,
        occurred_on_date,
        year,
        month,
        hour,
        day_of_week,
        is_shooting,
        time_of_day,
        is_weekend,
        season,
        year_month,
        crime_severity_tier,
        crime_severity_label,
        is_violent_crime,
        is_property_crime,
        is_drug_related,
        'BOSTON'        AS source_city
    FROM {{ ref('INT_BOSTON_CRIME') }}

    {% if is_incremental() %}
        WHERE occurred_on_date > (SELECT MAX(occurred_on_date) FROM {{ this }})
    {% endif %}
),

int_cambridge AS (
    SELECT
        incident_number,
        CAST(offense_code AS VARCHAR(20)) AS offense_code,
        offense_description,
        district,
        district_name,
        source_neighborhood,
        reporting_area,
        street,
        lat,
        long,
        valid_location,
        occurred_on_date,
        year,
        month,
        hour,
        day_of_week,
        is_shooting,
        time_of_day,
        is_weekend,
        season,
        year_month,
        crime_severity_tier,
        crime_severity_label,
        is_violent_crime,
        is_property_crime,
        is_drug_related,
        source_city
    FROM {{ ref('INT_CAMBRIDGE_CRIME') }}

    {% if is_incremental() %}
        WHERE occurred_on_date > (SELECT MAX(occurred_on_date) FROM {{ this }})
    {% endif %}
),

int_somerville AS (
    SELECT
        incident_number,
        offense_code,
        offense_description,
        district,
        district_name,
        'N/A'           AS source_neighborhood,
        reporting_area,
        street,
        lat,
        long,
        valid_location,
        occurred_on_date,
        year,
        month,
        hour,
        day_of_week,
        is_shooting,
        time_of_day,
        is_weekend,
        season,
        year_month,
        crime_severity_tier,
        crime_severity_label,
        is_violent_crime,
        is_property_crime,
        is_drug_related,
        'SOMERVILLE'   AS source_city
    FROM {{ ref('INT_SOMERVILLE_CRIME') }}

    {% if is_incremental() %}
        WHERE occurred_on_date > (SELECT MAX(occurred_on_date) FROM {{ this }})
    {% endif %}
),

-- Boston + Cambridge: need spatial join for neighborhood resolution
combined_boston_cambridge AS (
    SELECT * FROM int_boston
    UNION ALL
    SELECT * FROM int_cambridge
),

-- Geocoded coordinates for Boston crimes that originally had no lat/long.
-- ~14k rows recovered via Nominatim street address geocoding.
boston_crime_geocoded AS (
    SELECT
        INCIDENT_NUMBER,
        LAT  AS GEOCODED_LAT,
        LONG AS GEOCODED_LONG
    FROM {{ source('stage', 'stg_boston_crime_geocoded') }}
    WHERE GEOCODE_STATUS IN ('SUCCESS', 'SUCCESS_CACHED')
      AND LAT  IS NOT NULL AND LAT  != -999
      AND LONG IS NOT NULL AND LONG != -999
),

-- Overlay geocoded coords onto rows that were missing them
combined_with_geocoded AS (
    SELECT
        c.incident_number,
        c.offense_code,
        c.offense_description,
        c.district,
        c.district_name,
        c.source_neighborhood,
        c.reporting_area,
        c.street,
        CASE
            WHEN c.valid_location = FALSE AND g.GEOCODED_LAT IS NOT NULL
            THEN g.GEOCODED_LAT
            ELSE c.lat
        END AS lat,
        CASE
            WHEN c.valid_location = FALSE AND g.GEOCODED_LONG IS NOT NULL
            THEN g.GEOCODED_LONG
            ELSE c.long
        END AS long,
        CASE
            WHEN c.valid_location = TRUE THEN TRUE
            WHEN g.GEOCODED_LAT IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS valid_location,
        c.occurred_on_date,
        c.year,
        c.month,
        c.hour,
        c.day_of_week,
        c.is_shooting,
        c.time_of_day,
        c.is_weekend,
        c.season,
        c.year_month,
        c.crime_severity_tier,
        c.crime_severity_label,
        c.is_violent_crime,
        c.is_property_crime,
        c.is_drug_related,
        c.source_city
    FROM combined_boston_cambridge c
    LEFT JOIN boston_crime_geocoded g
        ON c.incident_number = g.INCIDENT_NUMBER
        AND c.valid_location = FALSE
),

master_location AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        granularity,
        geometry
    FROM {{ ref('MASTER_LOCATION') }}
    WHERE has_geometry = TRUE
),

-- INNER JOIN: only keep crimes that resolve to a MASTER_LOCATION polygon.
-- Drops rows with no valid coordinates and rows falling outside all polygons.
-- Every row in the MART has a definitive neighborhood assignment.
boston_cambridge_with_neighborhood AS (
    SELECT
        c.*,
        ml.location_id                  AS location_id,
        ml.neighborhood_name            AS neighborhood_name,
        ml.city                         AS city,
        'SPATIAL'                       AS resolution_method
    FROM combined_with_geocoded c
    INNER JOIN master_location ml
        ON c.valid_location = TRUE
        AND ST_CONTAINS(
            ml.geometry,
            ST_MAKEPOINT(
                CASE WHEN c.valid_location = TRUE THEN c.long ELSE 0 END,
                CASE WHEN c.valid_location = TRUE THEN c.lat  ELSE 0 END
            )
        )
),

-- Somerville: no lat/long → bypass spatial join, assign LOCATION_ID=40 directly.
-- Somerville is a single CITY polygon, so all crimes belong to it by definition.
somerville_with_neighborhood AS (
    SELECT
        s.*,
        40                              AS location_id,
        'SOMERVILLE'                    AS neighborhood_name,
        'SOMERVILLE'                    AS city,
        'CITY_ASSIGNMENT'               AS resolution_method
    FROM int_somerville s
),

-- Union all three cities
with_neighborhood AS (
    SELECT * FROM boston_cambridge_with_neighborhood
    UNION ALL
    SELECT * FROM somerville_with_neighborhood
),

with_description AS (
    SELECT
        incident_number,
        offense_code,
        offense_description,
        district,
        district_name,
        source_neighborhood,
        reporting_area,
        street,
        lat,
        long,
        valid_location,
        occurred_on_date,
        year,
        month,
        hour,
        day_of_week,
        is_shooting,
        time_of_day,
        is_weekend,
        season,
        year_month,
        crime_severity_tier,
        crime_severity_label,
        is_violent_crime,
        is_property_crime,
        is_drug_related,
        source_city,
        location_id,
        neighborhood_name,
        city,
        resolution_method,

        'A ' || crime_severity_label || ' severity ' ||
        LOWER(offense_description) || ' incident occurred in ' ||
        neighborhood_name || ', ' || city ||
        CASE
            WHEN street IS NOT NULL AND street != 'N/A'
            THEN ' on ' || street
            ELSE ''
        END ||
        ' on a ' || day_of_week || ' ' || time_of_day ||
        ' in ' || season || ' (' ||
        CASE month
            WHEN 1  THEN 'JANUARY'
            WHEN 2  THEN 'FEBRUARY'
            WHEN 3  THEN 'MARCH'
            WHEN 4  THEN 'APRIL'
            WHEN 5  THEN 'MAY'
            WHEN 6  THEN 'JUNE'
            WHEN 7  THEN 'JULY'
            WHEN 8  THEN 'AUGUST'
            WHEN 9  THEN 'SEPTEMBER'
            WHEN 10 THEN 'OCTOBER'
            WHEN 11 THEN 'NOVEMBER'
            WHEN 12 THEN 'DECEMBER'
            ELSE 'UNKNOWN'
        END || ', ' || CAST(year AS VARCHAR) || ').' ||
        CASE WHEN is_shooting      THEN ' A shooting was involved.'             ELSE '' END ||
        CASE WHEN is_violent_crime THEN ' This was classified as a violent crime.'  ELSE '' END ||
        CASE WHEN is_property_crime THEN ' This was classified as a property crime.' ELSE '' END ||
        CASE WHEN is_drug_related  THEN ' This incident was drug related.'      ELSE '' END
                                                                    AS row_description,

        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')        AS load_timestamp

    FROM with_neighborhood
)

SELECT * FROM with_description