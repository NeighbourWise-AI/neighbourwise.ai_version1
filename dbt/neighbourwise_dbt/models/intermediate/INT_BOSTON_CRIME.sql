{{
    config(
        materialized='incremental',
        unique_key='incident_number',
        schema='intermediate'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('stage', 'stg_boston_crime') }}
)

, district_mapping AS (
    SELECT * FROM {{ source('stage', 'stg_district_mapping') }}
)

, deduplicated AS (
    SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY INCIDENT_NUMBER, OFFENSE_CODE
                ORDER BY LOAD_TIMESTAMP DESC NULLS LAST
            ) AS row_num
        FROM source
        {% if is_incremental() %}
            WHERE OCCURRED_ON_DATE > (SELECT MAX(occurred_on_date) FROM {{ this }})
        {% endif %}
)

, cleaned AS (
    SELECT
        CAST(INCIDENT_NUMBER AS VARCHAR(20)) AS incident_number
        , CAST(OFFENSE_CODE AS INTEGER) AS offense_code
        , COALESCE(NULLIF(TRIM(UPPER(OFFENSE_DESCRIPTION)), ''),
                 'UNKNOWN') AS offense_description
        -- District code validated against known BPD districts
        , COALESCE(
            CASE
                WHEN UPPER(TRIM(deduplicated.DISTRICT)) IN (
                    'A1','A7','B2','B3','C6','C11','D4','D14','E5','E13','E18'
                ) THEN UPPER(TRIM(deduplicated.DISTRICT))
                ELSE NULL
            END,
            'UNKNOWN'
        ) AS district
        -- District name: joined from STG_DISTRICT_MAPPING
        -- Precise neighborhood resolution requires spatial join via MASTER_LOCATION using LAT/LONG
        , COALESCE(dm.DISTRICT_NAME, 'UNKNOWN') AS district_name
        , COALESCE(NULLIF(TRIM(UPPER(REPORTING_AREA)), ''), 'UNKNOWN') AS reporting_area
        , COALESCE(NULLIF(TRIM(UPPER(STREET)), ''), 'UNKNOWN') AS street
        , COALESCE(
            CASE
                WHEN LAT  BETWEEN 42.20 AND 42.55
                 AND LONG BETWEEN -71.35 AND -70.85
                THEN LAT
                ELSE NULL
            END,
            -999.0
        ) AS lat
        , COALESCE(
            CASE
                WHEN LAT  BETWEEN 42.20 AND 42.55
                 AND LONG BETWEEN -71.35 AND -70.85
                THEN LONG
                ELSE NULL
            END,
            -999.0
        ) AS long
        , CASE
            WHEN LAT  BETWEEN 42.20 AND 42.55
             AND LONG BETWEEN -71.35 AND -70.85
            THEN TRUE
            ELSE FALSE
        END AS valid_location
        , OCCURRED_ON_DATE AS occurred_on_date
        , COALESCE(CAST(YEAR AS INTEGER), -99) AS year
        , COALESCE(CAST(MONTH AS INTEGER), -99) AS month
        , COALESCE(CAST(HOUR AS INTEGER), -99) AS hour
        , COALESCE(NULLIF(TRIM(UPPER(DAY_OF_WEEK)), ''), 'UNKNOWN') AS day_of_week
        , CASE
            WHEN SHOOTING = 1 THEN TRUE
            ELSE FALSE
        END AS is_shooting
        , CASE
            WHEN HOUR BETWEEN 6  AND 11 THEN 'MORNING'
            WHEN HOUR BETWEEN 12 AND 16 THEN 'AFTERNOON'
            WHEN HOUR BETWEEN 17 AND 20 THEN 'EVENING'
            WHEN HOUR BETWEEN 21 AND 23 THEN 'NIGHT'
            WHEN HOUR BETWEEN 0  AND 5  THEN 'LATE NIGHT'
            ELSE 'UNKNOWN'
        END AS time_of_day
        , CASE
            WHEN UPPER(TRIM(DAY_OF_WEEK)) IN ('SATURDAY', 'SUNDAY') THEN TRUE
            ELSE FALSE
        END AS is_weekend
        , CASE
            WHEN MONTH IN (12, 1, 2)  THEN 'WINTER'
            WHEN MONTH IN (3, 4, 5)   THEN 'SPRING'
            WHEN MONTH IN (6, 7, 8)   THEN 'SUMMER'
            WHEN MONTH IN (9, 10, 11) THEN 'FALL'
            ELSE 'UNKNOWN'
        END AS season
        , CASE
            WHEN YEAR IS NOT NULL AND MONTH IS NOT NULL
            THEN CAST(YEAR AS VARCHAR) || '-' || LPAD(CAST(MONTH AS VARCHAR), 2, '0')
            ELSE 'UNKNOWN'
        END AS year_month
        , CASE
            WHEN UPPER(OFFENSE_DESCRIPTION) LIKE ANY (
                '%HOMICIDE%', '%MURDER%', '%MANSLAUGHTER%',
                '%RAPE%', '%SEXUAL ASSAULT%',
                '%ROBBERY%', '%AGGRAVATED ASSAULT%',
                '%ARSON%', '%BURGLARY%', '%AUTO THEFT%',
                '%KIDNAPPING%', '%HUMAN TRAFFICKING%'
            ) THEN 1
            WHEN UPPER(OFFENSE_DESCRIPTION) LIKE ANY (
                '%SIMPLE ASSAULT%', '%DRUG%', '%NARCOTIC%',
                '%FRAUD%', '%FORGERY%', '%EMBEZZLEMENT%',
                '%STOLEN PROPERTY%', '%VANDALISM%', '%WEAPON%',
                '%PROSTITUTION%', '%LIQUOR%', '%GAMBLING%',
                '%LARCENY%', '%HARASSMENT%', '%THREATS%'
            ) THEN 2
            ELSE 3
        END AS crime_severity_tier
        , CASE
            WHEN UPPER(OFFENSE_DESCRIPTION) LIKE ANY (
                '%HOMICIDE%', '%MURDER%', '%MANSLAUGHTER%',
                '%RAPE%', '%SEXUAL ASSAULT%',
                '%ROBBERY%', '%AGGRAVATED ASSAULT%',
                '%ARSON%', '%BURGLARY%', '%AUTO THEFT%',
                '%KIDNAPPING%', '%HUMAN TRAFFICKING%'
            ) THEN 'HIGH'
            WHEN UPPER(OFFENSE_DESCRIPTION) LIKE ANY (
                '%SIMPLE ASSAULT%', '%DRUG%', '%NARCOTIC%',
                '%FRAUD%', '%FORGERY%', '%EMBEZZLEMENT%',
                '%STOLEN PROPERTY%', '%VANDALISM%', '%WEAPON%',
                '%PROSTITUTION%', '%LIQUOR%', '%GAMBLING%',
                '%LARCENY%', '%HARASSMENT%', '%THREATS%'
            ) THEN 'MEDIUM'
            ELSE 'LOW'
        END AS crime_severity_label
        , CASE
            WHEN UPPER(OFFENSE_DESCRIPTION) LIKE ANY (
                '%HOMICIDE%', '%MURDER%', '%RAPE%', '%SEXUAL ASSAULT%',
                '%ROBBERY%', '%AGGRAVATED ASSAULT%', '%KIDNAPPING%',
                '%HUMAN TRAFFICKING%'
            ) THEN TRUE
            ELSE FALSE
        END AS is_violent_crime
        , CASE
            WHEN UPPER(OFFENSE_DESCRIPTION) LIKE ANY (
                '%LARCENY%', '%AUTO THEFT%', '%BURGLARY%',
                '%VANDALISM%', '%ARSON%', '%STOLEN PROPERTY%'
            ) THEN TRUE
            ELSE FALSE
        END AS is_property_crime
        , CASE
            WHEN UPPER(OFFENSE_DESCRIPTION) LIKE '%DRUG%'
              OR UPPER(OFFENSE_DESCRIPTION) LIKE '%NARCOTIC%'
            THEN TRUE
            ELSE FALSE
        END AS is_drug_related
        , TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp

    FROM deduplicated
    LEFT JOIN district_mapping dm
        ON UPPER(TRIM(deduplicated.DISTRICT)) = UPPER(TRIM(dm.DISTRICT_CODE))
    WHERE row_num = 1
)

SELECT *
FROM cleaned
WHERE occurred_on_date IS NOT NULL
AND occurred_on_date <= CURRENT_TIMESTAMP