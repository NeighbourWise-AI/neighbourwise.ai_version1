{{
    config(
        materialized='incremental',
        unique_key='incident_number',
        schema='intermediate'
    )
}}

WITH crime_source AS (
    SELECT * FROM {{ source('stage', 'stg_cambridge_crime') }}
),

geo_source AS (
    SELECT * FROM {{ source('stage', 'stg_cambridge_crime_geocoded') }}
),

-- Join crime to geocoded coords on FILE_NUMBER
-- Left join so ungeocoded rows still flow through (with -999 coords)
joined AS (
    SELECT
        c.*,
        COALESCE(g.LAT,  -999.0) AS geocoded_lat,
        COALESCE(g.LONG, -999.0) AS geocoded_long,
        COALESCE(g.GEOCODE_STATUS, 'NOT_GEOCODED') AS geocode_status
    FROM crime_source c
    LEFT JOIN geo_source g
        ON TRIM(c.FILE_NUMBER) = TRIM(g.FILE_NUMBER)
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            -- Cambridge has no OFFENSE_CODE — deduplicate on FILE_NUMBER only
            PARTITION BY FILE_NUMBER
            ORDER BY LOAD_TIMESTAMP DESC NULLS LAST
        ) AS row_num
    FROM joined
    {% if is_incremental() %}
        WHERE TRY_TO_TIMESTAMP(
                  SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                  'MM/DD/YYYY HH24:MI'
              ) > (SELECT MAX(occurred_on_date) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        -- FILE_NUMBER is the Cambridge primary key (equivalent to INCIDENT_NUMBER)
        CAST(FILE_NUMBER AS VARCHAR(20))                            AS incident_number,

        -- No numeric offense code in Cambridge — default -99
        -99                                                         AS offense_code,

        -- CRIME is the offense description field in Cambridge
        COALESCE(NULLIF(TRIM(UPPER(CRIME)), ''), 'UNKNOWN')        AS offense_description,

        -- No BPD district in Cambridge
        'N/A'                                                       AS district,
        'N/A'                                                       AS district_name,

        -- Source neighborhood kept for reference — spatial join is authoritative
        -- Peabody, Highlands, MIT, Inman/Harrington etc. may not be in MASTER_LOCATION
        COALESCE(NULLIF(TRIM(UPPER(NEIGHBORHOOD)), ''), 'UNKNOWN') AS source_neighborhood,

        -- REPORTING_AREA exists in Cambridge (numeric code)
        COALESCE(NULLIF(TRIM(UPPER(REPORTING_AREA)), ''), 'UNKNOWN') AS reporting_area,

        -- Street / Location field
        COALESCE(NULLIF(TRIM(UPPER(LOCATION)), ''), 'UNKNOWN')     AS street,

        -- Use geocoded lat/long from stg_cambridge_crime_geocoded
        -- Validate within greater Boston bounds
        COALESCE(
            CASE
                WHEN geocoded_lat  BETWEEN 42.20 AND 42.55
                 AND geocoded_long BETWEEN -71.35 AND -70.85
                THEN geocoded_lat
                ELSE NULL
            END,
            -999.0
        )                                                           AS lat,
        COALESCE(
            CASE
                WHEN geocoded_lat  BETWEEN 42.20 AND 42.55
                 AND geocoded_long BETWEEN -71.35 AND -70.85
                THEN geocoded_long
                ELSE NULL
            END,
            -999.0
        )                                                           AS long,
        CASE
            WHEN geocoded_lat  BETWEEN 42.20 AND 42.55
             AND geocoded_long BETWEEN -71.35 AND -70.85
            THEN TRUE
            ELSE FALSE
        END                                                         AS valid_location,

        -- Parse CRIME_DATE_TIME — take start time only (before ' - ' if range)
        -- Handles: "MM/DD/YYYY HH:MM", "MM/DD/YYYY HH:MM - HH:MM",
        --          and cross-day ranges like "12/31/2008 10:00 - 01/01/2009 10:00"
        TRY_TO_TIMESTAMP(
            SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
            'MM/DD/YYYY HH24:MI'
        )                                                           AS occurred_on_date,

        COALESCE(
            YEAR(TRY_TO_TIMESTAMP(
                SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                'MM/DD/YYYY HH24:MI'
            )), -99
        )                                                           AS year,

        COALESCE(
            MONTH(TRY_TO_TIMESTAMP(
                SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                'MM/DD/YYYY HH24:MI'
            )), -99
        )                                                           AS month,

        COALESCE(
            HOUR(TRY_TO_TIMESTAMP(
                SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                'MM/DD/YYYY HH24:MI'
            )), -99
        )                                                           AS hour,

        -- No DAY_OF_WEEK column in Cambridge — derive from parsed timestamp
        COALESCE(
            UPPER(DAYNAME(TRY_TO_TIMESTAMP(
                SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                'MM/DD/YYYY HH24:MI'
            ))),
            'UNKNOWN'
        )                                                           AS day_of_week,

        -- No SHOOTING column in Cambridge
        FALSE                                                       AS is_shooting,

        -- Time of day
        CASE
            WHEN HOUR(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) BETWEEN 6  AND 11 THEN 'MORNING'
            WHEN HOUR(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) BETWEEN 12 AND 16 THEN 'AFTERNOON'
            WHEN HOUR(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) BETWEEN 17 AND 20 THEN 'EVENING'
            WHEN HOUR(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) BETWEEN 21 AND 23 THEN 'NIGHT'
            WHEN HOUR(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) BETWEEN 0  AND 5  THEN 'LATE NIGHT'
            ELSE 'UNKNOWN'
        END                                                         AS time_of_day,

        -- Weekend flag — derived since no DAY_OF_WEEK source column
        CASE
            WHEN DAYOFWEEK(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) IN (1, 7) -- 1=Sunday, 7=Saturday in Snowflake
            THEN TRUE
            ELSE FALSE
        END                                                         AS is_weekend,

        -- Season
        CASE
            WHEN MONTH(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) IN (12, 1, 2)  THEN 'WINTER'
            WHEN MONTH(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) IN (3, 4, 5)   THEN 'SPRING'
            WHEN MONTH(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) IN (6, 7, 8)   THEN 'SUMMER'
            WHEN MONTH(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) IN (9, 10, 11) THEN 'FALL'
            ELSE 'UNKNOWN'
        END                                                         AS season,

        -- Year-month string
        CASE
            WHEN YEAR(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) IS NOT NULL
             AND MONTH(TRY_TO_TIMESTAMP(
                    SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                    'MM/DD/YYYY HH24:MI')) IS NOT NULL
            THEN CAST(YEAR(TRY_TO_TIMESTAMP(
                        SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                        'MM/DD/YYYY HH24:MI')) AS VARCHAR)
                 || '-'
                 || LPAD(CAST(MONTH(TRY_TO_TIMESTAMP(
                        SPLIT_PART(TRIM(CRIME_DATE_TIME), ' - ', 1),
                        'MM/DD/YYYY HH24:MI')) AS VARCHAR), 2, '0')
            ELSE 'UNKNOWN'
        END                                                         AS year_month,

        -- Severity — same logic as INT_BOSTON_CRIME, applied to CRIME field
        CASE
            WHEN UPPER(CRIME) LIKE ANY (
                '%HOMICIDE%', '%MURDER%', '%MANSLAUGHTER%',
                '%RAPE%', '%SEXUAL ASSAULT%',
                '%ROBBERY%', '%AGGRAVATED ASSAULT%',
                '%ARSON%', '%BURGLARY%', '%AUTO THEFT%',
                '%KIDNAPPING%', '%HUMAN TRAFFICKING%',
                '%HOUSEBREAK%'              -- Cambridge uses this term
            ) THEN 1
            WHEN UPPER(CRIME) LIKE ANY (
                '%SIMPLE ASSAULT%', '%DRUG%', '%NARCOTIC%',
                '%FRAUD%', '%FORGERY%', '%EMBEZZLEMENT%',
                '%STOLEN PROPERTY%', '%MAL. DEST%', '%VANDALISM%',
                '%WEAPON%', '%PROSTITUTION%', '%LIQUOR%', '%GAMBLING%',
                '%LARCENY%', '%HARASSMENT%', '%THREATS%', '%SHOPLIFTING%'
            ) THEN 2
            ELSE 3
        END                                                         AS crime_severity_tier,

        CASE
            WHEN UPPER(CRIME) LIKE ANY (
                '%HOMICIDE%', '%MURDER%', '%MANSLAUGHTER%',
                '%RAPE%', '%SEXUAL ASSAULT%',
                '%ROBBERY%', '%AGGRAVATED ASSAULT%',
                '%ARSON%', '%BURGLARY%', '%AUTO THEFT%',
                '%KIDNAPPING%', '%HUMAN TRAFFICKING%',
                '%HOUSEBREAK%'
            ) THEN 'HIGH'
            WHEN UPPER(CRIME) LIKE ANY (
                '%SIMPLE ASSAULT%', '%DRUG%', '%NARCOTIC%',
                '%FRAUD%', '%FORGERY%', '%EMBEZZLEMENT%',
                '%STOLEN PROPERTY%', '%MAL. DEST%', '%VANDALISM%',
                '%WEAPON%', '%PROSTITUTION%', '%LIQUOR%', '%GAMBLING%',
                '%LARCENY%', '%HARASSMENT%', '%THREATS%', '%SHOPLIFTING%'
            ) THEN 'MEDIUM'
            ELSE 'LOW'
        END                                                         AS crime_severity_label,

        CASE
            WHEN UPPER(CRIME) LIKE ANY (
                '%HOMICIDE%', '%MURDER%', '%RAPE%', '%SEXUAL ASSAULT%',
                '%ROBBERY%', '%AGGRAVATED ASSAULT%', '%KIDNAPPING%',
                '%HUMAN TRAFFICKING%'
            ) THEN TRUE
            ELSE FALSE
        END                                                         AS is_violent_crime,

        CASE
            WHEN UPPER(CRIME) LIKE ANY (
                '%LARCENY%', '%AUTO THEFT%', '%BURGLARY%',
                '%HOUSEBREAK%', '%MAL. DEST%', '%VANDALISM%',
                '%ARSON%', '%STOLEN PROPERTY%'
            ) THEN TRUE
            ELSE FALSE
        END                                                         AS is_property_crime,

        CASE
            WHEN UPPER(CRIME) LIKE '%DRUG%'
              OR UPPER(CRIME) LIKE '%NARCOTIC%'
            THEN TRUE
            ELSE FALSE
        END                                                         AS is_drug_related,

        -- Source city tag for downstream union with Boston
        'CAMBRIDGE'                                                 AS source_city,

        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')       AS load_timestamp

    FROM deduplicated
    WHERE row_num = 1
)

SELECT *
FROM cleaned
WHERE occurred_on_date IS NOT NULL
  AND occurred_on_date <= CURRENT_TIMESTAMP
  AND year != -99
  AND year >= 2023