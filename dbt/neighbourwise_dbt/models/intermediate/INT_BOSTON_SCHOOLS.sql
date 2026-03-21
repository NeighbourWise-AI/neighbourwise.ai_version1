{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('stage', 'stg_boston_schools') }}
),

-- Join geocoded coordinates — only keep schools that were successfully geocoded
-- Schools outside Greater Boston bounding box will have GEOCODE_STATUS = 'FAILED'
geocoded AS (
    SELECT
        SCHID,
        LAT,
        LONG
    FROM {{ source('stage', 'stg_boston_schools_geocoded') }}
    WHERE GEOCODE_STATUS = 'GEOCODED'
),

cleaned AS (
    SELECT
        s.SCHID AS school_id
        , COALESCE(NULLIF(TRIM(UPPER(s.NAME)), ''), 'UNKNOWN') AS school_name
        , COALESCE(NULLIF(TRIM(UPPER(s.ADDRESS)), ''), 'UNKNOWN') AS address
        , COALESCE(NULLIF(TRIM(UPPER(s.TOWN_MAIL)), ''), 'UNKNOWN') AS town_mail
        , COALESCE(NULLIF(TRIM(UPPER(s.TOWN)), ''), 'UNKNOWN') AS town
        , COALESCE(CAST(s.ZIPCODE AS VARCHAR(10)), 'UNKNOWN') AS zipcode
        , COALESCE(
            NULLIF(REGEXP_REPLACE(TRIM(s.PHONE), '[^0-9]', ''), ''),
            'UNKNOWN'
        ) AS phone
        , COALESCE(
            CASE
                WHEN UPPER(TRIM(s.TYPE)) IN (
                    'CHA','ELE','MID','OTH','PRI',
                    'SEC','SPE','SPU','UNK','VOC'
                ) THEN UPPER(TRIM(s.TYPE))
                ELSE NULL
            END,
            'UNKNOWN'
        ) AS school_type_code
        , COALESCE(NULLIF(TRIM(UPPER(s.TYPE_DESC)), ''), 'UNKNOWN') AS school_type_desc
        , CASE
            WHEN UPPER(TRIM(s.TYPE)) IN (
                'ELE','MID','SEC','OTH','UNK','VOC'
            ) THEN TRUE
            ELSE FALSE
        END AS is_public
        , CASE
            WHEN UPPER(TRIM(s.TYPE)) = 'PRI' THEN TRUE
            ELSE FALSE
        END AS is_private
        , CASE
            WHEN UPPER(TRIM(s.TYPE)) = 'CHA' THEN TRUE
            ELSE FALSE
        END AS is_charter
        , CASE
            WHEN UPPER(TRIM(s.TYPE)) IN ('SPE', 'SPU') THEN TRUE
            ELSE FALSE
        END AS is_special_education
        , CASE
            WHEN UPPER(TRIM(s.TYPE)) = 'VOC' THEN TRUE
            ELSE FALSE
        END AS is_vocational
        , COALESCE(NULLIF(TRIM(s.GRADES), ''), 'UNKNOWN') AS grades_raw
        , COALESCE(NULLIF(SPLIT_PART(TRIM(s.GRADES), ',', 1), ''), 'UNKNOWN') AS grade_low
        , COALESCE(
            NULLIF(
                CAST(GET(SPLIT(TRIM(s.GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(s.GRADES), ',')) - 1) AS VARCHAR),
                ''
            ),
            'UNKNOWN'
        ) AS grade_high
        , CASE
            WHEN UPPER(TRIM(s.TYPE)) = 'ELE'
              OR UPPER(SPLIT_PART(TRIM(s.GRADES), ',', 1)) = 'PK'
              OR (
                TRY_CAST(SPLIT_PART(TRIM(s.GRADES), ',', 1) AS INTEGER) BETWEEN 1 AND 5
                AND TRY_CAST(CAST(GET(SPLIT(TRIM(s.GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(s.GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) <= 6
              ) THEN 'ELEMENTARY'
            WHEN UPPER(TRIM(s.TYPE)) = 'MID'
              OR (
                TRY_CAST(SPLIT_PART(TRIM(s.GRADES), ',', 1) AS INTEGER) BETWEEN 6 AND 8
                AND TRY_CAST(CAST(GET(SPLIT(TRIM(s.GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(s.GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) <= 9
              ) THEN 'MIDDLE'
            WHEN UPPER(TRIM(s.TYPE)) IN ('SEC', 'VOC')
              OR (
                TRY_CAST(SPLIT_PART(TRIM(s.GRADES), ',', 1) AS INTEGER) BETWEEN 9 AND 12
              ) THEN 'HIGH'
            WHEN (
                UPPER(SPLIT_PART(TRIM(s.GRADES), ',', 1)) = 'PK'
                OR TRY_CAST(SPLIT_PART(TRIM(s.GRADES), ',', 1) AS INTEGER) <= 5
              )
              AND TRY_CAST(CAST(GET(SPLIT(TRIM(s.GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(s.GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) >= 9
            THEN 'K-12'
            ELSE 'UNKNOWN'
        END AS school_level
        , CASE
            WHEN UPPER(SPLIT_PART(TRIM(s.GRADES), ',', 1)) = 'PK'
              OR TRY_CAST(SPLIT_PART(TRIM(s.GRADES), ',', 1) AS INTEGER) <= 5
            THEN TRUE
            ELSE FALSE
        END AS serves_elementary
        , CASE
            WHEN (
                UPPER(SPLIT_PART(TRIM(s.GRADES), ',', 1)) = 'PK'
                OR TRY_CAST(SPLIT_PART(TRIM(s.GRADES), ',', 1) AS INTEGER) <= 8
              )
             AND TRY_CAST(CAST(GET(SPLIT(TRIM(s.GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(s.GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) >= 6
            THEN TRUE
            ELSE FALSE
        END AS serves_middle
        , CASE
            WHEN TRY_CAST(CAST(GET(SPLIT(TRIM(s.GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(s.GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) >= 9
            THEN TRUE
            ELSE FALSE
        END AS serves_high
        , COALESCE(CAST(s.MA_ADDR_ID AS INTEGER), -99) AS ma_addr_id
        , COALESCE(g.LAT, -999.0)  AS lat
        , COALESCE(g.LONG, -999.0) AS long
        , CASE
            WHEN g.LAT IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_valid_location

        , TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp

    FROM source s
    INNER JOIN geocoded g
        ON s.SCHID = g.SCHID
)

SELECT *
FROM cleaned
WHERE school_id IS NOT NULL