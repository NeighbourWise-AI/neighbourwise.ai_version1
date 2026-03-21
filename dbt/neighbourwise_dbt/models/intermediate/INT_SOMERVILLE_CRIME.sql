{{
    config(
        materialized='incremental',
        unique_key='incident_number',
        schema='intermediate'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('stage', 'stg_somerville_crime') }}
)

, deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY INCIDENT_NUMBER, OFFENSE_CODE
            ORDER BY LOAD_TIMESTAMP DESC NULLS LAST
        ) AS row_num
    FROM source
    WHERE DAY_AND_MONTH IS NOT NULL
      AND TRIM(DAY_AND_MONTH) != ''
    {% if is_incremental() %}
        AND LOAD_TIMESTAMP > (SELECT MAX(load_timestamp) FROM {{ this }})
    {% endif %}
)

-- Construct occurred_on_date once to avoid repeating TRY_TO_DATE everywhere
, with_date AS (
    SELECT
        *,
        TRY_TO_DATE(
            CAST(YEAR AS VARCHAR) || '/' || DAY_AND_MONTH,
            'YYYY/MM/DD'
        ) AS constructed_date,
        CAST(SPLIT_PART(DAY_AND_MONTH, '/', 1) AS INTEGER) AS parsed_month
    FROM deduplicated
    WHERE row_num = 1
)

, cleaned AS (
    SELECT
        -- ============================================================
        -- Match INT_BOSTON_CRIME output columns exactly (same order)
        -- ============================================================
        CAST(INCIDENT_NUMBER AS VARCHAR(20)) AS incident_number
        , CAST(OFFENSE_CODE AS VARCHAR(20)) AS offense_code
        , COALESCE(NULLIF(TRIM(UPPER(OFFENSE)), ''), 'UNKNOWN') AS offense_description
        , 'N/A' AS district
        , 'N/A' AS district_name
        , 'N/A' AS reporting_area
        , 'N/A' AS street
        , -999.0 AS lat
        , -999.0 AS long
        , FALSE AS valid_location
        , constructed_date AS occurred_on_date
        , CAST(YEAR AS INTEGER) AS year
        , parsed_month AS month

        -- Hour: approximate from police_shift (midpoint of each shift)
        , CASE
            WHEN UPPER(TRIM(POLICE_SHIFT)) = 'DAY SHIFT (8AM - 4PM)' THEN 12
            WHEN UPPER(TRIM(POLICE_SHIFT)) = 'FIRST HALF (4PM - MIDNIGHT)' THEN 20
            WHEN UPPER(TRIM(POLICE_SHIFT)) = 'LAST HALF (MIDNIGHT - 8AM)' THEN 4
            ELSE -99
          END AS hour

        -- Day of week: DAYNAME returns 'Mon','Tue',etc — convert to full names to match Boston
        , CASE UPPER(DAYNAME(constructed_date))
            WHEN 'MON' THEN 'MONDAY'
            WHEN 'TUE' THEN 'TUESDAY'
            WHEN 'WED' THEN 'WEDNESDAY'
            WHEN 'THU' THEN 'THURSDAY'
            WHEN 'FRI' THEN 'FRIDAY'
            WHEN 'SAT' THEN 'SATURDAY'
            WHEN 'SUN' THEN 'SUNDAY'
            ELSE 'UNKNOWN'
          END AS day_of_week

        , FALSE AS is_shooting

        , CASE
            WHEN UPPER(TRIM(POLICE_SHIFT)) = 'DAY SHIFT (8AM - 4PM)' THEN 'AFTERNOON'
            WHEN UPPER(TRIM(POLICE_SHIFT)) = 'FIRST HALF (4PM - MIDNIGHT)' THEN 'EVENING'
            WHEN UPPER(TRIM(POLICE_SHIFT)) = 'LAST HALF (MIDNIGHT - 8AM)' THEN 'LATE NIGHT'
            ELSE 'UNKNOWN'
          END AS time_of_day

        , CASE UPPER(DAYNAME(constructed_date))
            WHEN 'SAT' THEN TRUE
            WHEN 'SUN' THEN TRUE
            ELSE FALSE
          END AS is_weekend

        , CASE
            WHEN parsed_month IN (12, 1, 2)  THEN 'WINTER'
            WHEN parsed_month IN (3, 4, 5)   THEN 'SPRING'
            WHEN parsed_month IN (6, 7, 8)   THEN 'SUMMER'
            WHEN parsed_month IN (9, 10, 11) THEN 'FALL'
            ELSE 'UNKNOWN'
          END AS season

        , CAST(YEAR AS VARCHAR) || '-' || LPAD(CAST(parsed_month AS VARCHAR), 2, '0') AS year_month

        , CASE
            WHEN UPPER(OFFENSE) LIKE ANY (
                '%HOMICIDE%', '%MURDER%', '%MANSLAUGHTER%',
                '%RAPE%', '%SEXUAL ASSAULT%',
                '%ROBBERY%', '%AGGRAVATED ASSAULT%',
                '%ARSON%', '%BURGLARY%', '%AUTO THEFT%',
                '%KIDNAPPING%', '%HUMAN TRAFFICKING%'
            ) THEN 1
            WHEN UPPER(OFFENSE) LIKE ANY (
                '%SIMPLE ASSAULT%', '%DRUG%', '%NARCOTIC%',
                '%FRAUD%', '%FORGERY%', '%EMBEZZLEMENT%',
                '%STOLEN PROPERTY%', '%VANDALISM%', '%WEAPON%',
                '%PROSTITUTION%', '%LIQUOR%', '%GAMBLING%',
                '%LARCENY%', '%HARASSMENT%', '%THREATS%',
                '%SWINDLE%', '%IDENTITY THEFT%', '%COUNTERFEIT%',
                '%DESTRUCTION%', '%SHOPLIFTING%', '%THEFT%',
                '%TRESPASS%', '%DISORDERLY%'
            ) THEN 2
            ELSE 3
          END AS crime_severity_tier

        , CASE
            WHEN UPPER(OFFENSE) LIKE ANY (
                '%HOMICIDE%', '%MURDER%', '%MANSLAUGHTER%',
                '%RAPE%', '%SEXUAL ASSAULT%',
                '%ROBBERY%', '%AGGRAVATED ASSAULT%',
                '%ARSON%', '%BURGLARY%', '%AUTO THEFT%',
                '%KIDNAPPING%', '%HUMAN TRAFFICKING%'
            ) THEN 'HIGH'
            WHEN UPPER(OFFENSE) LIKE ANY (
                '%SIMPLE ASSAULT%', '%DRUG%', '%NARCOTIC%',
                '%FRAUD%', '%FORGERY%', '%EMBEZZLEMENT%',
                '%STOLEN PROPERTY%', '%VANDALISM%', '%WEAPON%',
                '%PROSTITUTION%', '%LIQUOR%', '%GAMBLING%',
                '%LARCENY%', '%HARASSMENT%', '%THREATS%',
                '%SWINDLE%', '%IDENTITY THEFT%', '%COUNTERFEIT%',
                '%DESTRUCTION%', '%SHOPLIFTING%', '%THEFT%',
                '%TRESPASS%', '%DISORDERLY%'
            ) THEN 'MEDIUM'
            ELSE 'LOW'
          END AS crime_severity_label

        , CASE
            WHEN UPPER(OFFENSE) LIKE ANY (
                '%HOMICIDE%', '%MURDER%', '%RAPE%', '%SEXUAL ASSAULT%',
                '%ROBBERY%', '%AGGRAVATED ASSAULT%', '%KIDNAPPING%',
                '%HUMAN TRAFFICKING%'
            ) THEN TRUE
            ELSE FALSE
          END AS is_violent_crime

        , CASE
            WHEN UPPER(OFFENSE) LIKE ANY (
                '%LARCENY%', '%AUTO THEFT%', '%BURGLARY%',
                '%VANDALISM%', '%ARSON%', '%STOLEN PROPERTY%',
                '%MOTOR VEHICLE THEFT%', '%DESTRUCTION%',
                '%SHOPLIFTING%', '%THEFT%'
            ) THEN TRUE
            ELSE FALSE
          END AS is_property_crime

        , CASE
            WHEN UPPER(OFFENSE) LIKE '%DRUG%'
              OR UPPER(OFFENSE) LIKE '%NARCOTIC%'
            THEN TRUE
            ELSE FALSE
          END AS is_drug_related

        , TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp

    FROM with_date
)

SELECT *
FROM cleaned
WHERE occurred_on_date IS NOT NULL
  AND occurred_on_date <= CURRENT_DATE
  AND YEAR >= 2023