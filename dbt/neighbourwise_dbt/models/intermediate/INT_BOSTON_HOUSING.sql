{{ config(materialized='table', schema='INTERMEDIATE') }}

/*
    Intermediate model: INT_BOSTON_HOUSING
    Transformations applied:
      1. UPPER() on all text columns
      2. COALESCE(..., 'UNKNOWN') on text columns / NULL on numeric columns
      3. Extract label from coded values like 'A - Average' → 'AVERAGE'
      4. VARCHAR → proper numeric types:
           INT    : counts, floors, units, areas, years, fireplaces, parking
           DECIMAL: dollar values (land/bldg/total value, tax) — strips $, commas, spaces
      5. Derived metrics: estimated_rent, price_per_sqft, property_age,
                          renovation_recency_score, is_parking
*/

{% set text_cols = [
    'OWNER','CITY','MAIL_CITY','MAIL_STATE','ST_NAME','UNIT_NUM',
    'LU_DESC','BLDG_TYPE','OWN_OCC','MAIL_ADDRESSEE','MAIL_STREET_ADDRESS','MAIL_ZIP_CODE'
] %}

{% set coded_cols = [
    'STRUCTURE_CLASS','ROOF_STRUCTURE','ROOF_COVER','INT_WALL','EXT_FNISHED',
    'INT_COND','EXT_COND','OVERALL_COND','BDRM_COND',
    'BTHRM_STYLE1','BTHRM_STYLE2','BTHRM_STYLE3',
    'KITCHEN_TYPE','KITCHEN_STYLE1','KITCHEN_STYLE2','KITCHEN_STYLE3',
    'HEAT_TYPE','HEAT_SYSTEM','AC_TYPE','ORIENTATION','PROP_VIEW','CORNER_UNIT'
] %}

-- ─────────────────────────────────────────────────────────────────────────────
-- Helper macros (inline)
--   clean_int(col)      : strips commas/spaces → TRY_CAST AS INT
--   clean_decimal(col)  : strips $, commas, spaces → TRY_CAST AS DECIMAL(18,2)
-- ─────────────────────────────────────────────────────────────────────────────

with base as (
    select * from {{ source('STAGE', 'STG_BOSTON_HOUSING') }}
),

cleaned as (
    select

        -- ── IDENTITY / IDs (stay VARCHAR) ────────────────────────────────────
          COALESCE(NULLIF(TRIM(_ID),    ''), 'UNKNOWN')  as id
        , COALESCE(NULLIF(TRIM(PID),    ''), 'UNKNOWN')  as pid
        , COALESCE(NULLIF(TRIM(CM_ID),  ''), 'UNKNOWN')  as cm_id
        , COALESCE(NULLIF(TRIM(GIS_ID), ''), 'UNKNOWN')  as gis_id

        -- ── ADDRESS (stay VARCHAR) ────────────────────────────────────────────
        , COALESCE(NULLIF(TRIM(ST_NUM),   ''), 'UNKNOWN')                   as st_num
        , COALESCE(NULLIF(TRIM(ST_NUM2),  ''), 'UNKNOWN')                   as st_num2
        , UPPER(COALESCE(NULLIF(TRIM(ST_NAME),  ''), 'UNKNOWN'))            as st_name
        , UPPER(COALESCE(NULLIF(TRIM(UNIT_NUM), ''), 'UNKNOWN'))            as unit_num
        , UPPER(COALESCE(NULLIF(TRIM(CITY),     ''), 'UNKNOWN'))            as city
        , COALESCE(NULLIF(TRIM(ZIP_CODE), ''), 'UNKNOWN')                   as zip_code

        -- ── BUILDING IDENTIFIERS (stay VARCHAR) ───────────────────────────────
        , COALESCE(NULLIF(TRIM(BLDG_SEQ),  ''), 'UNKNOWN')                  as bldg_seq
        , COALESCE(NULLIF(TRIM(LUC),       ''), 'UNKNOWN')                  as luc
        , UPPER(COALESCE(NULLIF(TRIM(LU),       ''), 'UNKNOWN'))            as lu
        , UPPER(COALESCE(NULLIF(TRIM(LU_DESC),  ''), 'UNKNOWN'))            as lu_desc
        , UPPER(COALESCE(NULLIF(TRIM(BLDG_TYPE),''), 'UNKNOWN'))            as bldg_type
        , UPPER(COALESCE(NULLIF(TRIM(OWN_OCC),  ''), 'UNKNOWN'))            as own_occ

        -- ── OWNER / MAIL (stay VARCHAR) ───────────────────────────────────────
        , UPPER(COALESCE(NULLIF(TRIM(OWNER),               ''), 'UNKNOWN')) as owner
        , UPPER(COALESCE(NULLIF(TRIM(MAIL_ADDRESSEE),      ''), 'UNKNOWN')) as mail_addressee
        , UPPER(COALESCE(NULLIF(TRIM(MAIL_STREET_ADDRESS), ''), 'UNKNOWN')) as mail_street_address
        , UPPER(COALESCE(NULLIF(TRIM(MAIL_CITY),           ''), 'UNKNOWN')) as mail_city
        , UPPER(COALESCE(NULLIF(TRIM(MAIL_STATE),          ''), 'UNKNOWN')) as mail_state
        , COALESCE(NULLIF(TRIM(MAIL_ZIP_CODE), ''), 'UNKNOWN')              as mail_zip_code

        -- ── COUNTS / FLOORS / UNITS  →  INT ──────────────────────────────────
        -- Pattern: strip commas & spaces, TRY_CAST to INT (NULL if non-numeric)
        , TRY_CAST(REPLACE(TRIM(NUM_BLDGS),  ',', '') AS INT)   as num_bldgs
        , TRY_CAST(REPLACE(TRIM(RES_FLOOR),  ',', '') AS INT)   as res_floor
        , TRY_CAST(REPLACE(TRIM(CD_FLOOR),   ',', '') AS INT)   as cd_floor
        , TRY_CAST(REPLACE(TRIM(RES_UNITS),  ',', '') AS INT)   as res_units
        , TRY_CAST(REPLACE(TRIM(COM_UNITS),  ',', '') AS INT)   as com_units
        , TRY_CAST(REPLACE(TRIM(RC_UNITS),   ',', '') AS INT)   as rc_units

        -- ── AREA FIELDS  →  INT (sq ft, whole numbers) ────────────────────────
        , TRY_CAST(REPLACE(TRIM(LAND_SF),     ',', '') AS INT)  as land_sf
        , TRY_CAST(REPLACE(TRIM(GROSS_AREA),  ',', '') AS INT)  as gross_area
        , TRY_CAST(REPLACE(TRIM(LIVING_AREA), ',', '') AS INT)  as living_area

        -- ── DOLLAR VALUES  →  DECIMAL(18,2) ──────────────────────────────────
        -- Pattern: strip leading/trailing spaces, then $, then commas
        , TRY_CAST(REPLACE(REPLACE(REPLACE(TRIM(LAND_VALUE),  ' ',''), '$',''), ',','') AS DECIMAL(18,2))  as land_value
        , TRY_CAST(REPLACE(REPLACE(REPLACE(TRIM(BLDG_VALUE),  ' ',''), '$',''), ',','') AS DECIMAL(18,2))  as bldg_value
        , TRY_CAST(REPLACE(REPLACE(REPLACE(TRIM(SFYI_VALUE),  ' ',''), '$',''), ',','') AS DECIMAL(18,2))  as sfyi_value
        , TRY_CAST(REPLACE(REPLACE(REPLACE(TRIM(TOTAL_VALUE), ' ',''), '$',''), ',','') AS DECIMAL(18,2))  as total_value
        , TRY_CAST(REPLACE(REPLACE(REPLACE(TRIM(GROSS_TAX),   ' ',''), '$',''), ',','') AS DECIMAL(18,2))  as gross_tax

        -- ── YEAR FIELDS  →  INT ───────────────────────────────────────────────
        , TRY_CAST(TRIM(YR_BUILT) AS INT)                                as yr_built
        , TRY_CAST(TRIM(YR_REMODEL) AS INT)                                as yr_remodel

        -- ── ROOM / FEATURE COUNTS  →  INT ─────────────────────────────────────
        , TRY_CAST(TRIM(BED_RMS) AS INT)                                as bed_rms
        , TRY_CAST(TRIM(FULL_BTH) AS INT)                                as full_bth
        , TRY_CAST(TRIM(HLF_BTH) AS INT)                                as hlf_bth
        , TRY_CAST(TRIM(KITCHENS) AS INT)                                as kitchens
        , TRY_CAST(TRIM(TT_RMS) AS INT)                                as tt_rms
        , TRY_CAST(TRIM(FIREPLACES) AS INT)                                as fireplaces
        , TRY_CAST(TRIM(NUM_PARKING) AS INT)                                as num_parking

        -- ── CODED COLUMNS  →  VARCHAR (label only, UPPER) ────────────────────
        {% for col in coded_cols %}
        , UPPER(COALESCE(
            NULLIF(
                CASE
                    WHEN TRIM({{ col }}) LIKE '% - %'
                        THEN TRIM(SPLIT_PART(TRIM({{ col }}), ' - ', 2))
                    ELSE TRIM({{ col }})
                END,
            ''),
          'UNKNOWN')) as {{ col | lower }}
        {% if not loop.last %} {% endif %}
        {% endfor %}

    from base
),

-- ─────────────────────────────────────────────────────────────────────────────
-- DERIVED METRICS
-- Now that numeric columns are proper types, no string stripping needed here
-- ─────────────────────────────────────────────────────────────────────────────
metrics as (
    select
        c.*,

        -- ── 1. ESTIMATED RENT  →  DECIMAL(18,2) ──────────────────────────────
        -- 0.8% of total_value per month (Boston gross rent multiplier)
        ROUND(c.total_value * 0.008, 2)                      as estimated_rent,

        -- ── 2. PRICE PER SQFT  →  DECIMAL(18,2) ──────────────────────────────
        CASE
            WHEN c.living_area IS NULL OR c.living_area = 0 THEN NULL
            WHEN c.total_value IS NULL                       THEN NULL
            ELSE ROUND(c.total_value / c.living_area, 2)
        END                                                   as price_per_sqft,

        -- ── 3. PROPERTY AGE  →  INT ───────────────────────────────────────────
        CASE
            WHEN c.yr_built IS NULL                          THEN NULL
            WHEN c.yr_built > YEAR(CURRENT_DATE)             THEN NULL
            ELSE YEAR(CURRENT_DATE) - c.yr_built
        END                                                   as property_age,

        -- ── 4. RENOVATION RECENCY SCORE  →  INT (1–10) ───────────────────────
        -- Uses yr_remodel if available, falls back to yr_built with -2 penalty
        CASE
            WHEN c.yr_remodel IS NOT NULL
             AND c.yr_remodel <= YEAR(CURRENT_DATE)
            THEN
                CASE
                    WHEN YEAR(CURRENT_DATE) - c.yr_remodel = 0   THEN 10
                    WHEN YEAR(CURRENT_DATE) - c.yr_remodel <= 2  THEN 9
                    WHEN YEAR(CURRENT_DATE) - c.yr_remodel <= 5  THEN 8
                    WHEN YEAR(CURRENT_DATE) - c.yr_remodel <= 10 THEN 7
                    WHEN YEAR(CURRENT_DATE) - c.yr_remodel <= 15 THEN 6
                    WHEN YEAR(CURRENT_DATE) - c.yr_remodel <= 20 THEN 5
                    WHEN YEAR(CURRENT_DATE) - c.yr_remodel <= 30 THEN 4
                    WHEN YEAR(CURRENT_DATE) - c.yr_remodel <= 40 THEN 3
                    WHEN YEAR(CURRENT_DATE) - c.yr_remodel <= 50 THEN 2
                    ELSE 1
                END
            WHEN c.yr_built IS NOT NULL
             AND c.yr_built <= YEAR(CURRENT_DATE)
            THEN
                GREATEST(1,
                    CASE
                        WHEN YEAR(CURRENT_DATE) - c.yr_built = 0   THEN 10
                        WHEN YEAR(CURRENT_DATE) - c.yr_built <= 2  THEN 9
                        WHEN YEAR(CURRENT_DATE) - c.yr_built <= 5  THEN 8
                        WHEN YEAR(CURRENT_DATE) - c.yr_built <= 10 THEN 7
                        WHEN YEAR(CURRENT_DATE) - c.yr_built <= 15 THEN 6
                        WHEN YEAR(CURRENT_DATE) - c.yr_built <= 20 THEN 5
                        WHEN YEAR(CURRENT_DATE) - c.yr_built <= 30 THEN 4
                        WHEN YEAR(CURRENT_DATE) - c.yr_built <= 40 THEN 3
                        WHEN YEAR(CURRENT_DATE) - c.yr_built <= 50 THEN 2
                        ELSE 1
                    END
                - 2)
            ELSE NULL
        END                                                   as renovation_recency_score,

        -- ── 5. IS PARKING FLAG  →  VARCHAR ───────────────────────────────────
        CASE
            WHEN c.num_parking IS NULL     THEN 'F'
            WHEN c.num_parking > 0         THEN 'T'
            ELSE                                'F'
        END                                                   as is_parking

    from cleaned c
)

select * from metrics