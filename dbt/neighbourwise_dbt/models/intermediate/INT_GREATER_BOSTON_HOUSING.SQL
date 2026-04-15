{{
    config(
        materialized = 'table',
        schema       = 'INTERMEDIATE',
        tags         = ['housing', 'greater_boston', 'intermediate']
    )
}}

/*
================================================================================
INT_GREATER_BOSTON_HOUSING
--------------------------------------------------------------------------------
Source : STAGE.STG_GREATER_BOSTON_HOUSING
Cities : Quincy (84), Malden (147), Medford (148),
         Arlington (5), Watertown (53), Salem (223)
Rows   : 22,953 loaded 2026-03-25

Source columns confirmed from MassGIS L3 API (outFields=*):
  OBJECTID, MAP_PAR_ID, LOC_ID, POLY_TYPE, MAP_NO, SOURCE, PLAN_ID, LAST_EDIT,
  BND_CHK, NO_MATCH, TOWN_ID, PROP_ID, BLDG_VAL, LAND_VAL, OTHER_VAL,
  TOTAL_VAL, FY, LOT_SIZE, LS_DATE, LS_PRICE, USE_CODE, SITE_ADDR, ADDR_NUM,
  FULL_STR, LOCATION, CITY, ZIP, OWNER1, OWN_ADDR, OWN_CITY, OWN_STATE,
  OWN_ZIP, OWN_CO, LS_BOOK, LS_PAGE, REG_ID, ZONING, YEAR_BUILT, BLD_AREA,
  UNITS, RES_AREA, STYLE, NUM_ROOMS, STORIES, SHAPE_AREA, SHAPE_LEN,
  LATITUDE, LONGITUDE (last 2 extracted from geometry centroid)

Key differences from Boston/Cambridge intermediates:
  - No coded prefixes (e.g. 'M - MANSARD') — STYLE is plain text
  - No currency symbols — BLDG_VAL / TOTAL_VAL are plain integers
  - LAT/LONG extracted from geometry centroid during data load
  - USE_CODE is a MassGIS state use code integer (101=Single Family, 102=Condo)
  - CONDITION not available in L3 — defaults to UNKNOWN / -99
  - AC, parking, fireplace not available in L3 — default false
================================================================================
*/

with source as (
    select * from {{ source('STAGE', 'STG_GREATER_BOSTON_HOUSING') }}
),

-- Use master location centroids as coordinates for all Greater Boston properties
-- The per-property coordinates from the ArcGIS L3 API are unreliable (Web Mercator
-- bleed-through even with outSR=4326). Since each city maps 1:1 to a master location,
-- the city centroid is the correct coordinate for the spatial join.
city_centroids as (
    select
        upper(trim(name))    as city_name,
        location_id,
        centroid_lat         as city_lat,
        centroid_long        as city_long
    from {{ source('STAGE', 'STG_MASTER_LOCATION') }}
    where upper(trim(name)) in (
        'QUINCY','MALDEN','MEDFORD','ARLINGTON','WATERTOWN','SALEM',
        'BROOKLINE','CHELSEA','EVERETT','SOMERVILLE','REVERE'
    )
),

casted as (
    select

        -- ── Primary keys ──────────────────────────────────────────────────
        coalesce(nullif(trim(s.loc_id),    ''), 'UNKNOWN')              as property_id,
        coalesce(nullif(trim(s.prop_id),   ''), 'UNKNOWN')              as gis_id,
        coalesce(try_cast(nullif(trim(s.town_id), '') as integer), -99) as town_id,

        -- ── Address ───────────────────────────────────────────────────────
        coalesce(nullif(trim(upper(s.site_addr)), ''), 'UNKNOWN')       as full_address,
        coalesce(nullif(trim(upper(s.addr_num)),  ''), 'UNKNOWN')       as street_num,
        coalesce(nullif(trim(upper(s.full_str)),  ''), 'UNKNOWN')       as street_name,
        'UNKNOWN'                                                        as unit_num,
        -- CITY field stores postal/mailing city, not municipality name
        -- Derive actual municipality from TOWN_ID
        case try_cast(nullif(trim(s.town_id), '') as float)::integer
            when 10  then 'ARLINGTON'
            when 46  then 'BROOKLINE'
            when 57  then 'CHELSEA'
            when 93  then 'EVERETT'
            when 165 then 'MALDEN'
            when 176 then 'MEDFORD'
            when 243 then 'QUINCY'
            when 248 then 'REVERE'
            when 258 then 'SALEM'
            when 274 then 'SOMERVILLE'
            when 314 then 'WATERTOWN'
            else upper(trim(s.city))
        end                                                             as city,
        -- L3 ZIP stores mailing/postal ZIP which is unreliable for municipality identification
        -- Always use the known correct municipal ZIP derived from TOWN_ID
        case try_cast(nullif(trim(s.town_id), '') as float)::integer
            when 10  then '02476'   -- Arlington
            when 46  then '02445'   -- Brookline
            when 57  then '02150'   -- Chelsea
            when 93  then '02149'   -- Everett
            when 165 then '02148'   -- Malden
            when 176 then '02155'   -- Medford
            when 243 then '02169'   -- Quincy
            when 248 then '02151'   -- Revere
            when 258 then '01970'   -- Salem
            when 274 then '02143'   -- Somerville
            when 314 then '02472'   -- Watertown
            else coalesce(nullif(trim(s.zip), ''), 'UNKNOWN')
        end                                                             as zip_code,

        -- ── Property classification ───────────────────────────────────────
        -- MassGIS state use codes:
        --   100 = Undeveloped land
        --   101 = Single Family
        --   102 = Condominium
        --   104 = Two Family
        --   105 = Three Family
        --   111-113 = 4-8 unit apartments
        --   300-399 = Commercial
        --   900+    = Exempt (municipal, govt, religious)
        coalesce(nullif(trim(s.use_code),      ''), 'UNKNOWN')          as state_use_code,
        coalesce(nullif(trim(upper(s.style)),  ''), 'UNKNOWN')          as building_type,
        coalesce(nullif(trim(upper(s.zoning)), ''), 'UNKNOWN')          as zoning,
        case
            -- 3-digit codes: 100-199 = Residential
            when try_cast(trim(s.use_code) as integer) between 100 and 199
                then 'RESIDENTIAL'
            -- 4-digit codes: 1000-1999 = Residential (MassGIS L3 extended)
            when try_cast(trim(s.use_code) as integer) between 1000 and 1999
                then 'RESIDENTIAL'
            -- 3-digit commercial: 300-399
            when try_cast(trim(s.use_code) as integer) between 300 and 399
                then 'COMMERCIAL'
            -- 4-digit commercial: 3000-3999
            when try_cast(trim(s.use_code) as integer) between 3000 and 3999
                then 'COMMERCIAL'
            -- Exempt: 900+ (3-digit) or 9000+ (4-digit)
            when try_cast(trim(s.use_code) as integer) >= 900
                then 'EXEMPT'
            else 'OTHER'
        end                                                             as property_class,

        -- ── Ownership ─────────────────────────────────────────────────────
        coalesce(nullif(trim(upper(s.owner1)), ''), 'UNKNOWN')          as owner_name,

        -- ── Building metrics ──────────────────────────────────────────────
        coalesce(try_cast(nullif(trim(s.lot_size),  '') as integer), -99) as land_sqft,
        coalesce(try_cast(nullif(trim(s.res_area),  '') as integer), -99) as living_area_sqft,
        coalesce(try_cast(nullif(trim(s.bld_area),  '') as integer), -99) as gross_area_sqft,
        coalesce(try_cast(nullif(trim(s.units),     '') as integer), -99) as residential_units,
        coalesce(try_cast(nullif(trim(s.stories),   '') as float),   -99) as num_stories,
        coalesce(try_cast(nullif(trim(s.num_rooms), '') as integer), -99) as total_rooms,

        -- ── Valuation ─────────────────────────────────────────────────────
        -- L3 stores plain integers — no $ stripping required
        coalesce(try_cast(nullif(trim(s.land_val),  '') as float), -999.0) as land_value,
        coalesce(try_cast(nullif(trim(s.bldg_val),  '') as float), -999.0) as building_value,
        coalesce(try_cast(nullif(trim(s.other_val), '') as float), -999.0) as other_value,
        coalesce(try_cast(nullif(trim(s.total_val), '') as float), -999.0) as total_assessed_value,
        coalesce(try_cast(nullif(trim(s.ls_price),  '') as float), -999.0) as sale_price,
        coalesce(try_cast(nullif(trim(s.fy),        '') as integer), -99)  as assessment_year,

        -- ── Property age ──────────────────────────────────────────────────
        coalesce(try_cast(nullif(trim(s.year_built), '') as integer), -99) as year_built,

        -- ── Derived metrics ───────────────────────────────────────────────
        -- Estimated rent: tiered GRM based on assessed value
        -- Lower multiplier for higher-value properties (luxury market reality)
        --   > $2M:      0.25% → realistic for ultra-luxury condos
        --   $1M-$2M:    0.30% → realistic for premium Boston/Cambridge
        --   $700K-$1M:  0.40% → realistic for mid-high market
        --   $400K-$700K: 0.50% → realistic for mid market
        --   < $400K:    0.60% → realistic for affordable market
        case
            when try_cast(nullif(trim(s.total_val), '') as float) > 2000000
            then round(try_cast(nullif(trim(s.total_val), '') as float) * 0.0025, 2)
            when try_cast(nullif(trim(s.total_val), '') as float) > 1000000
            then round(try_cast(nullif(trim(s.total_val), '') as float) * 0.0030, 2)
            when try_cast(nullif(trim(s.total_val), '') as float) > 700000
            then round(try_cast(nullif(trim(s.total_val), '') as float) * 0.0040, 2)
            when try_cast(nullif(trim(s.total_val), '') as float) > 400000
            then round(try_cast(nullif(trim(s.total_val), '') as float) * 0.0050, 2)
            when try_cast(nullif(trim(s.total_val), '') as float) > 0
            then round(try_cast(nullif(trim(s.total_val), '') as float) * 0.0060, 2)
            else -999.0
        end                                                             as estimated_rent,

        -- Price per sqft
        case
            when try_cast(nullif(trim(s.res_area),  '') as float) > 0
                and try_cast(nullif(trim(s.total_val), '') as float) > 0
            then round(
                try_cast(nullif(trim(s.total_val), '') as float) /
                try_cast(nullif(trim(s.res_area),  '') as float),
                2
            )
            else -999.0
        end                                                             as price_per_sqft,

        -- Property age (as of 2026)
        case
            when try_cast(nullif(trim(s.year_built), '') as integer) > 0
                and try_cast(nullif(trim(s.year_built), '') as integer) <= 2026
            then (2026 - try_cast(nullif(trim(s.year_built), '') as integer))
            else -99
        end                                                             as property_age,

        -- ── Coordinates ───────────────────────────────────────────────────
        -- Use master location city centroid — per-property API coords are unreliable
        coalesce(cc.city_lat,  -999.0)                                  as lat,
        coalesce(cc.city_long, -999.0)                                  as long,
        case
            when cc.city_lat  between 41.2 and 42.9
                and cc.city_long between -73.5 and -69.9
            then true
            else false
        end                                                             as has_valid_location

    from source s
    left join city_centroids cc
        on case try_cast(nullif(trim(s.town_id), '') as float)::integer
               when 10  then 'ARLINGTON'
               when 46  then 'BROOKLINE'
               when 57  then 'CHELSEA'
               when 93  then 'EVERETT'
               when 165 then 'MALDEN'
               when 176 then 'MEDFORD'
               when 243 then 'QUINCY'
               when 248 then 'REVERE'
               when 258 then 'SALEM'
               when 274 then 'SOMERVILLE'
               when 314 then 'WATERTOWN'
           end = cc.city_name
    where
        s.loc_id is not null
        -- Filter to our 6 target municipalities
        -- TOWN_ID may be stored as '84' or '84.0' (float string from ArcGIS API)
        -- Mailing CITY is not reliable — use TOWN_ID as primary, mailing city as fallback
        and (
            -- Correct MassGIS TOWN_IDs (integer string format)
            trim(s.town_id) in ('10','46','57','93','165','176','243','248','258','274','314')
            -- Float string format e.g. '10.0'
            or trim(s.town_id) in ('10.0','46.0','57.0','93.0','165.0','176.0',
                                   '243.0','248.0','258.0','274.0','314.0')
            -- Mailing city fallback (L3 CITY field stores postal city not municipality)
            or upper(trim(s.city)) in (
                'AGAWAM',          -- Arlington mail routing
                'ORANGE',          -- Salem mail routing
                'LANCASTER',       -- Malden mail routing
                'LANESBOROUGH',    -- Medford mail routing
                'EAST BROOKFIELD', -- Quincy mail routing
                'CHARLEMONT',      -- Watertown mail routing
                'BROOKLINE','CHELSEA','EVERETT','SOMERVILLE','REVERE'
            )
        )
        -- Exclude road ROW, water bodies, and non-parcel polygons
        and coalesce(nullif(trim(s.poly_type), ''), 'FEE') = 'FEE'
),

final as (
    select
        *,

        -- ── Residential type flags ────────────────────────────────────────
        case
            when state_use_code = '102'
                or building_type ilike '%CONDO%'
            then true else false
        end                                                             as is_condo,

        case
            when state_use_code in ('104', '105', '111', '112', '113')
                or building_type ilike '%MULTI%'
                or building_type ilike '%TWO%'
                or building_type ilike '%THREE%'
                or building_type ilike '%APT%'
                or building_type ilike '%APARTMENT%'
                or building_type ilike '%GARDEN%'
                or building_type ilike '%TOWNHOUSE%'
            then true else false
        end                                                             as is_rental_property,

        case
            when property_class = 'RESIDENTIAL' then true
            else false
        end                                                             as is_residential,

        -- ── Amenity flags ─────────────────────────────────────────────────
        -- L3 Assess table does not include AC, parking, or fireplace fields
        false                                                           as has_ac,
        false                                                           as has_parking_space,
        false                                                           as has_fireplace,

        case
            when residential_units > 0
                or building_type ilike '%PARK%'
            then true else false
        end                                                             as is_parking,

        -- ── Condition ─────────────────────────────────────────────────────
        -- L3 does not include condition ratings — carried as sentinel values
        'UNKNOWN'                                                       as overall_condition,
        -99                                                             as condition_score,

        -- ── Source identifier ─────────────────────────────────────────────
        'GREATER_BOSTON'                                                as data_source,

        current_timestamp()                                             as dbt_loaded_at

    from casted
)

select * from final