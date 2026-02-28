{{
    config(
        materialized = 'table',
        schema       = 'INTERMEDIATE',
        tags         = ['grocery', 'intermediate']
    )
}}

/*
================================================================================
INT_BOSTON_GROCERY_STORES
--------------------------------------------------------------------------------
Source : STAGE.STG_BOSTON_GROCERY_STORES
Layer  : Intermediate
--------------------------------------------------------------------------------
Transformations applied:
  1. Data type casting         — objectid → INTEGER, year → INTEGER, zip cleaned
  2. NULL / empty handling     — sentinels: 'UNKNOWN' for strings, -99 for ints
  3. Standardization           — UPPER(), TRIM(), consistent store_type labels
  4. Derived columns           — naics_category, store_type_clean, is_essential,
                                  is_pharmacy, is_specialty, data_vintage,
                                  zip_valid, naics_major_group
  5. Deduplication             — on (coname, staddr, stcity, zip, year)
                                  keeps row with lowest objectid
  6. Shape column dropped      — proprietary Esri binary, not decodable
================================================================================
*/

with

source as (
    select * from {{ source('stage', 'STG_BOSTON_GROCERY_STORES') }}
),

-- ─────────────────────────────────────────────
-- STEP 1: Cast, clean, and sentinel-fill
-- ─────────────────────────────────────────────
casted as (
    select
        -- Primary key
        try_cast(objectid as integer)                                   as objectid,

        -- Store identity
        upper(trim(coname))                                             as store_name,
        upper(trim(staddr))                                             as street_address,
        upper(trim(stcity))                                             as city,

        -- ZIP: strip whitespace, validate 5-digit format
        case
            when regexp_like(trim(zip), '^[0-9]{5}$')
                and trim(zip) != '00000'
            then trim(zip)
            else 'UNKNOWN'
        end                                                             as zip_code,

        -- Location license number (only populated for 2021 data)
        case
            when trim(locnum) = '' or locnum is null then 'UNKNOWN'
            else trim(locnum)
        end                                                             as location_license_num,

        -- NAICS code (only populated for 2021 data)
        case
            when trim(naics) = '' or naics is null then 'UNKNOWN'
            else trim(naics)
        end                                                             as naics_code,

        -- Store type — raw (kept for reference)
        upper(trim(store_type))                                         as store_type_raw,

        -- Year as integer
        try_cast(year as integer)                                       as data_year

    from source
    where objectid is not null
),

-- ─────────────────────────────────────────────
-- STEP 2: Standardize store_type labels
-- (collapse duplicates and fix inconsistent naming)
-- ─────────────────────────────────────────────
standardized as (
    select
        *,
        case
            when store_type_raw in (
                'SUPERMARKET OR OTHER GROCERY',
                'SUPERMARKETS/OTHER GROCERY (EXC CONVENIENCE) STRS'
            )                                               then 'SUPERMARKET'
            when store_type_raw in (
                'CONVENIENCE STORES',
                'CONVENIENCE STORES, PHARMACIES, AND DRUG STORES'
            )                                               then 'CONVENIENCE_STORE'
            when store_type_raw in (
                'MEAT MARKETS, FISH AND SEAFOOD MARKETS, AND ALL OTHER SPECIALTY FOOD STORES',
                'MEAT MARKETS'
            )                                               then 'MEAT_MARKET'
            when store_type_raw in (
                'FISH & SEAFOOD MARKETS'
            )                                               then 'FISH_SEAFOOD_MARKET'
            when store_type_raw in (
                'FRUIT AND VEGETABLE MARKETS',
                'FRUIT & VEGETABLE MARKETS'
            )                                               then 'FRUIT_VEG_MARKET'
            when store_type_raw in (
                'ALL OTHER SPECIALTY FOOD STORES'
            )                                               then 'SPECIALTY_FOOD_STORE'
            when store_type_raw in (
                'PHARMACIES & DRUG STORES'
            )                                               then 'PHARMACY'
            when store_type_raw in (
                'WAREHOUSE CLUBS & SUPERCENTERS',
                'WAREHOUSE CLUBS AND SUPERCENTERS'
            )                                               then 'WAREHOUSE_CLUB'
            when store_type_raw = 'FARMERS MARKETS'         then 'FARMERS_MARKET'
            when store_type_raw = 'WINTER MARKETS'          then 'WINTER_MARKET'
            when store_type_raw = 'DOLLAR STORE'            then 'DOLLAR_STORE'
            when store_type_raw = 'DEPARTMENT STORES (EXCEPT DISCOUNT DEPT STORES)'
                                                            then 'DEPARTMENT_STORE'
            else 'OTHER'
        end                                                             as store_type_clean,

        -- NAICS 2-digit major group description
        case
            when left(naics_code, 5) = '44511' then 'Supermarkets & Grocery'
            when left(naics_code, 5) = '44512' then 'Convenience Stores'
            when left(naics_code, 5) = '44521' then 'Meat Markets'
            when left(naics_code, 5) = '44522' then 'Fish & Seafood Markets'
            when left(naics_code, 5) = '44523' then 'Fruit & Vegetable Markets'
            when left(naics_code, 5) = '44529' then 'Other Specialty Food'
            when left(naics_code, 5) = '44611' then 'Pharmacies & Drug Stores'
            when left(naics_code, 5) in ('45221','45231') then 'Warehouse & Department Stores'
            when naics_code = 'UNKNOWN'         then 'UNKNOWN'
            else 'Other Retail'
        end                                                             as naics_category

    from casted
),

-- ─────────────────────────────────────────────
-- STEP 3: Deduplication
-- Keep lowest objectid per (store_name, street_address, city, zip_code, data_year)
-- ─────────────────────────────────────────────
deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by store_name, street_address, city, zip_code, data_year
                order by objectid asc
            ) as _row_num
        from standardized
    )
    where _row_num = 1
),

-- ─────────────────────────────────────────────
-- STEP 4: Derived / business logic columns
-- ─────────────────────────────────────────────
final as (
    select
        -- ── Keys ──────────────────────────────
        objectid,

        -- ── Store identity ────────────────────
        store_name,
        street_address,
        city,
        zip_code,
        location_license_num,

        -- ── Classification ────────────────────
        naics_code,
        naics_category,
        store_type_raw,
        store_type_clean,

        -- ── Data vintage ──────────────────────
        data_year,
        case
            when data_year = 2021 then '2021 Survey'
            when data_year = 2017 then '2017 Survey'
            else 'UNKNOWN'
        end                                                             as data_vintage,

        -- ── Derived flags ─────────────────────

        -- Is this a primary grocery source (food security relevance)?
        case
            when store_type_clean in (
                'SUPERMARKET', 'FRUIT_VEG_MARKET', 'MEAT_MARKET',
                'FISH_SEAFOOD_MARKET', 'WAREHOUSE_CLUB', 'FARMERS_MARKET',
                'WINTER_MARKET', 'SPECIALTY_FOOD_STORE'
            ) then true
            else false
        end                                                             as is_essential_food_source,

        -- Is this a pharmacy / drug store?
        case
            when store_type_clean in ('PHARMACY', 'CONVENIENCE_STORE')
                or naics_category = 'Pharmacies & Drug Stores'
            then true else false
        end                                                             as is_pharmacy_or_drug_store,

        -- Is this a specialty / niche food store?
        case
            when store_type_clean in (
                'FRUIT_VEG_MARKET', 'MEAT_MARKET', 'FISH_SEAFOOD_MARKET',
                'SPECIALTY_FOOD_STORE', 'FARMERS_MARKET', 'WINTER_MARKET'
            ) then true else false
        end                                                             as is_specialty_store,

        -- Is this a large-format store?
        case
            when store_type_clean in ('SUPERMARKET', 'WAREHOUSE_CLUB', 'DEPARTMENT_STORE')
            then true else false
        end                                                             as is_large_format,

        -- Is this a convenience / quick-stop store?
        case
            when store_type_clean in ('CONVENIENCE_STORE', 'DOLLAR_STORE')
            then true else false
        end                                                             as is_convenience_type,

        -- ZIP validity flag
        case
            when zip_code = 'UNKNOWN' then false else true
        end                                                             as is_zip_valid,

        -- Data completeness flag (2021 has locnum + naics, 2017 does not)
        case
            when naics_code != 'UNKNOWN'
                and location_license_num != 'UNKNOWN'
            then true else false
        end                                                             as is_fully_attributed,

        -- ── Metadata ──────────────────────────
        current_timestamp()                                             as dbt_loaded_at

    from deduped
)

select * from final