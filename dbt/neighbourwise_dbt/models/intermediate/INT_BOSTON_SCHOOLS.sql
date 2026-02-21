{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('stage', 'stg_boston_schools') }}
)

, cleaned AS (
    SELECT
        SCHID AS school_id 
        , COALESCE(NULLIF(TRIM(UPPER(NAME)), ''), 'UNKNOWN') AS school_name
        , COALESCE(NULLIF(TRIM(UPPER(ADDRESS)), ''), 'UNKNOWN') AS address
        , COALESCE(NULLIF(TRIM(UPPER(TOWN_MAIL)), ''), 'UNKNOWN') AS town_mail
        , COALESCE(NULLIF(TRIM(UPPER(TOWN)), ''), 'UNKNOWN') AS town
        , COALESCE(CAST(ZIPCODE AS VARCHAR(10)), 'UNKNOWN')  AS zipcode
        , COALESCE(
            NULLIF(REGEXP_REPLACE(TRIM(PHONE), '[^0-9]', ''), ''),
            'UNKNOWN'
        ) AS phone
        , COALESCE(
            CASE
                WHEN UPPER(TRIM(TYPE)) IN (
                    'CHA','ELE','MID','OTH','PRI',
                    'SEC','SPE','SPU','UNK','VOC'
                ) THEN UPPER(TRIM(TYPE))
                ELSE NULL
            END,
            'UNKNOWN'
        ) AS school_type_code
        , COALESCE(NULLIF(TRIM(UPPER(TYPE_DESC)), ''), 'UNKNOWN') AS school_type_desc
        , CASE
            WHEN UPPER(TRIM(TYPE)) IN (
                'ELE','MID','SEC','OTH','UNK','VOC'
            ) THEN TRUE
            ELSE FALSE
        END AS is_public
        , CASE
            WHEN UPPER(TRIM(TYPE)) = 'PRI' THEN TRUE
            ELSE FALSE
        END AS is_private
        , CASE
            WHEN UPPER(TRIM(TYPE)) = 'CHA' THEN TRUE
            ELSE FALSE
        END AS is_charter
        , CASE
            WHEN UPPER(TRIM(TYPE)) IN ('SPE', 'SPU') THEN TRUE
            ELSE FALSE
        END AS is_special_education
        , CASE
            WHEN UPPER(TRIM(TYPE)) = 'VOC' THEN TRUE
            ELSE FALSE
        END AS is_vocational
        , COALESCE(NULLIF(TRIM(GRADES), ''), 'UNKNOWN') AS grades_raw
        , COALESCE(NULLIF(SPLIT_PART(TRIM(GRADES), ',', 1), ''), 'UNKNOWN') AS grade_low
        , COALESCE(
            NULLIF(
                CAST(GET(SPLIT(TRIM(GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(GRADES), ',')) - 1) AS VARCHAR),
                ''
            ),
            'UNKNOWN'
        ) AS grade_high
        , CASE
            WHEN UPPER(TRIM(TYPE)) = 'ELE'
              OR UPPER(SPLIT_PART(TRIM(GRADES), ',', 1)) = 'PK'
              OR (
                TRY_CAST(SPLIT_PART(TRIM(GRADES), ',', 1) AS INTEGER) BETWEEN 1 AND 5
                AND TRY_CAST(CAST(GET(SPLIT(TRIM(GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) <= 6
              ) THEN 'ELEMENTARY'
            WHEN UPPER(TRIM(TYPE)) = 'MID'
              OR (
                TRY_CAST(SPLIT_PART(TRIM(GRADES), ',', 1) AS INTEGER) BETWEEN 6 AND 8
                AND TRY_CAST(CAST(GET(SPLIT(TRIM(GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) <= 9
              ) THEN 'MIDDLE'
            WHEN UPPER(TRIM(TYPE)) IN ('SEC', 'VOC')
              OR (
                TRY_CAST(SPLIT_PART(TRIM(GRADES), ',', 1) AS INTEGER) BETWEEN 9 AND 12
              ) THEN 'HIGH'
            WHEN (
                UPPER(SPLIT_PART(TRIM(GRADES), ',', 1)) = 'PK'
                OR TRY_CAST(SPLIT_PART(TRIM(GRADES), ',', 1) AS INTEGER) <= 5
              )
              AND TRY_CAST(CAST(GET(SPLIT(TRIM(GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) >= 9
            THEN 'K-12'
            ELSE 'UNKNOWN'
        END AS school_level
        , CASE
            WHEN UPPER(SPLIT_PART(TRIM(GRADES), ',', 1)) = 'PK'
              OR TRY_CAST(SPLIT_PART(TRIM(GRADES), ',', 1) AS INTEGER) <= 5
            THEN TRUE
            ELSE FALSE
        END AS serves_elementary
        , CASE
            WHEN (
                UPPER(SPLIT_PART(TRIM(GRADES), ',', 1)) = 'PK'
                OR TRY_CAST(SPLIT_PART(TRIM(GRADES), ',', 1) AS INTEGER) <= 8
              )
             AND TRY_CAST(CAST(GET(SPLIT(TRIM(GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) >= 6
            THEN TRUE
            ELSE FALSE
        END AS serves_middle
        , CASE
            WHEN TRY_CAST(CAST(GET(SPLIT(TRIM(GRADES), ','), ARRAY_SIZE(SPLIT(TRIM(GRADES), ',')) - 1) AS VARCHAR) AS INTEGER) >= 9
            THEN TRUE
            ELSE FALSE
        END AS serves_high
        , COALESCE(CAST(MA_ADDR_ID AS INTEGER), -99) AS ma_addr_id
        , TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp
    FROM source
)

SELECT *
FROM cleaned
WHERE school_id IS NOT NULL