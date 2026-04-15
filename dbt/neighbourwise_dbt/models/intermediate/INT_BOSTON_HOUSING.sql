{{
    config(
        materialized = 'table',
        schema       = 'INTERMEDIATE',
        tags         = ['housing', 'intermediate']
    )
}}

/*
================================================================================
INT_BOSTON_HOUSING
--------------------------------------------------------------------------------
Sources: STAGE.STG_BOSTON_HOUSING
         STAGE.STG_BOSTON_HOUSING_GEOCODED  (lat/long via Nominatim)
Layer  : Intermediate
--------------------------------------------------------------------------------
Transformations applied:
  1. Columns dropped      — mail/admin fields, near-zero fill columns
                            (RC_UNITS, COM_UNITS, SFYI_VALUE, STRUCTURE_CLASS,
                             ST_NUM2, BTHRM_STYLE2/3, KITCHEN_STYLE2/3,
                             MAIL_ADDRESSEE, GIS_ID, CM_ID)
  2. Type casting          — integers, floats, booleans via TRY_CAST
  3. NULL / sentinel fill  — 'UNKNOWN' for strings, -99 for ints, -999.0 floats
  4. Standardization       — UPPER(), TRIM(), consistent labels
  5. Derived flags         — is_condo, is_rental, is_owner_occupied,
                             is_commercial, has_ac, has_parking,
                             is_recently_renovated, condition_score
  6. Geocode join          — INNER JOIN on ID to STG_BOSTON_HOUSING_GEOCODED
                             only GEOCODED rows retained
================================================================================
*/

with

source as (
    select * from {{ source('STAGE', 'STG_BOSTON_HOUSING') }}
),

geocoded as (
    select
        pid,
        lat,
        long
    from {{ source('STAGE', 'STG_BOSTON_HOUSING_GEOCODED') }}
    where geocode_status = 'GEOCODED'
),

-- ─────────────────────────────────────────────
-- STEP 1: Cast, clean, sentinel-fill
-- INNER JOIN geocoded: only properties with valid coordinates retained
-- ─────────────────────────────────────────────
casted as (
    select
        -- ── Keys ──────────────────────────────
        trim(s.pid)                                                     as property_id,
        coalesce(nullif(trim(s.gis_id), ''), 'UNKNOWN')                 as gis_id,

        -- ── Address ───────────────────────────
        coalesce(nullif(trim(s.st_num), ''), 'UNKNOWN')                 as street_num,
        coalesce(nullif(trim(upper(s.st_name)),  ''), 'UNKNOWN')        as street_name,
        coalesce(nullif(trim(upper(s.unit_num)), ''), 'UNKNOWN')        as unit_num,
        coalesce(nullif(trim(upper(s.city)),     ''), 'UNKNOWN')        as city,
        case
            when regexp_like(trim(s.zip_code), '^[0-9]{5}$')
            then trim(s.zip_code)
            else 'UNKNOWN'
        end                                                             as zip_code,

        -- ── Land use classification ───────────
        coalesce(nullif(trim(upper(s.lu)),      ''), 'UNKNOWN')         as land_use_code,
        coalesce(nullif(trim(upper(s.lu_desc)), ''), 'UNKNOWN')         as land_use_desc,
        coalesce(nullif(trim(upper(s.luc)),     ''), 'UNKNOWN')         as luc,
        case
            when trim(s.bldg_type) ilike '%-%'
            then coalesce(nullif(trim(upper(split_part(trim(s.bldg_type), ' - ', 2))), ''), 'UNKNOWN')
            else coalesce(nullif(trim(upper(s.bldg_type)), ''), 'UNKNOWN')
        end                                                             as building_type,

        -- ── Ownership ─────────────────────────
        coalesce(nullif(trim(upper(s.owner)), ''), 'UNKNOWN')           as owner_name,
        case
            when upper(trim(s.own_occ)) = 'Y' then true
            else false
        end                                                             as is_owner_occupied,

        -- ── Building metrics ──────────────────
        coalesce(try_cast(s.num_bldgs   as integer), -99)               as num_buildings,
        coalesce(try_cast(s.bldg_seq    as integer), -99)               as building_seq,
        coalesce(try_cast(s.res_floor   as integer), -99)               as residential_floor,
        coalesce(try_cast(s.cd_floor    as integer), -99)               as condo_floor,
        coalesce(try_cast(s.res_units   as integer), -99)               as residential_units,
        coalesce(try_cast(s.land_sf     as integer), -99)               as land_sqft,
        coalesce(try_cast(s.gross_area  as integer), -99)               as gross_area_sqft,
        coalesce(try_cast(s.living_area as integer), -99)               as living_area_sqft,

        -- ── Valuation ─────────────────────────
        coalesce(
            try_cast(
                replace(replace(nullif(trim(s.land_value), ''), ',', ''), chr(36), '')
            as float),
        -999.0)                                                         as land_value,
        coalesce(
            try_cast(
                replace(replace(nullif(trim(s.bldg_value), ''), ',', ''), chr(36), '')
            as float),
        -999.0)                                                         as building_value,
        coalesce(
            try_cast(
                replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '')
            as float),
        -999.0)                                                         as total_assessed_value,
        coalesce(
            try_cast(
                replace(replace(nullif(trim(s.gross_tax), ''), ',', ''), chr(36), '')
            as float),
        -999.0)                                                         as gross_tax,

        -- ── Property characteristics ──────────
        coalesce(try_cast(s.yr_built    as integer), -99)               as year_built,
        coalesce(try_cast(s.yr_remodel  as integer), -99)               as year_remodel,
        coalesce(try_cast(s.bed_rms     as integer), -99)               as bedrooms,
        coalesce(try_cast(s.full_bth    as integer), -99)               as full_baths,
        coalesce(try_cast(s.hlf_bth     as integer), -99)               as half_baths,
        coalesce(try_cast(s.kitchens    as integer), -99)               as kitchens,
        coalesce(try_cast(s.tt_rms      as integer), -99)               as total_rooms,
        coalesce(try_cast(s.fireplaces  as integer), -99)               as fireplaces,
        coalesce(try_cast(s.num_parking as integer), -99)               as num_parking,

        -- ── Physical attributes ───────────────
        case
            when nullif(trim(s.roof_structure), '') is null then 'UNKNOWN'
            when trim(s.roof_structure) like '% - %'
            then upper(trim(split_part(trim(s.roof_structure), ' - ', 2)))
            else upper(trim(s.roof_structure))
        end                                                             as roof_structure,
        case
            when nullif(trim(s.roof_cover), '') is null then 'UNKNOWN'
            when trim(s.roof_cover) like '% - %'
            then upper(trim(split_part(trim(s.roof_cover), ' - ', 2)))
            else upper(trim(s.roof_cover))
        end                                                             as roof_cover,
        case
            when nullif(trim(s.int_wall), '') is null then 'UNKNOWN'
            when trim(s.int_wall) like '% - %'
            then upper(trim(split_part(trim(s.int_wall), ' - ', 2)))
            else upper(trim(s.int_wall))
        end                                                             as interior_wall,
        case
            when nullif(trim(s.ext_fnished), '') is null then 'UNKNOWN'
            when trim(s.ext_fnished) like '% - %'
            then upper(trim(split_part(trim(s.ext_fnished), ' - ', 2)))
            else upper(trim(s.ext_fnished))
        end                                                             as exterior_finish,
        case
            when nullif(trim(s.heat_type), '') is null then 'UNKNOWN'
            when trim(s.heat_type) like '% - %'
            then upper(trim(split_part(trim(s.heat_type), ' - ', 2)))
            else upper(trim(s.heat_type))
        end                                                             as heat_type,
        case
            when nullif(trim(s.heat_system), '') is null then 'UNKNOWN'
            when trim(s.heat_system) like '% - %'
            then upper(trim(split_part(trim(s.heat_system), ' - ', 2)))
            else upper(trim(s.heat_system))
        end                                                             as heat_system,
        case
            when nullif(trim(s.ac_type), '') is null then 'UNKNOWN'
            when trim(s.ac_type) like '% - %'
            then upper(trim(split_part(trim(s.ac_type), ' - ', 2)))
            else upper(trim(s.ac_type))
        end                                                             as ac_type,

        -- ── Condition ─────────────────────────
        case
            when nullif(trim(s.overall_cond), '') is null then 'UNKNOWN'
            when trim(s.overall_cond) like '% - %'
            then upper(trim(split_part(trim(s.overall_cond), ' - ', 2)))
            else upper(trim(s.overall_cond))
        end                                                             as overall_condition,
        case
            when nullif(trim(s.int_cond), '') is null then 'UNKNOWN'
            when trim(s.int_cond) like '% - %'
            then upper(trim(split_part(trim(s.int_cond), ' - ', 2)))
            else upper(trim(s.int_cond))
        end                                                             as interior_condition,
        case
            when nullif(trim(s.ext_cond), '') is null then 'UNKNOWN'
            when trim(s.ext_cond) like '% - %'
            then upper(trim(split_part(trim(s.ext_cond), ' - ', 2)))
            else upper(trim(s.ext_cond))
        end                                                             as exterior_condition,
        case
            when nullif(trim(s.bdrm_cond), '') is null then 'UNKNOWN'
            when trim(s.bdrm_cond) like '% - %'
            then upper(trim(split_part(trim(s.bdrm_cond), ' - ', 2)))
            else upper(trim(s.bdrm_cond))
        end                                                             as bedroom_condition,

        -- ── Unit style ────────────────────────
        case
            when nullif(trim(s.bthrm_style1), '') is null then 'UNKNOWN'
            when trim(s.bthrm_style1) like '% - %'
            then upper(trim(split_part(trim(s.bthrm_style1), ' - ', 2)))
            else upper(trim(s.bthrm_style1))
        end                                                             as bathroom_style,
        case
            when nullif(trim(s.kitchen_type), '') is null then 'UNKNOWN'
            when trim(s.kitchen_type) like '% - %'
            then upper(trim(split_part(trim(s.kitchen_type), ' - ', 2)))
            else upper(trim(s.kitchen_type))
        end                                                             as kitchen_type,
        case
            when nullif(trim(s.kitchen_style1), '') is null then 'UNKNOWN'
            when trim(s.kitchen_style1) like '% - %'
            then upper(trim(split_part(trim(s.kitchen_style1), ' - ', 2)))
            else upper(trim(s.kitchen_style1))
        end                                                             as kitchen_style,
        case
            when nullif(trim(s.orientation), '') is null then 'UNKNOWN'
            when trim(s.orientation) like '% - %'
            then upper(trim(split_part(trim(s.orientation), ' - ', 2)))
            else upper(trim(s.orientation))
        end                                                             as orientation,
        case
            when nullif(trim(s.prop_view), '') is null then 'UNKNOWN'
            when trim(s.prop_view) like '% - %'
            then upper(trim(split_part(trim(s.prop_view), ' - ', 2)))
            else upper(trim(s.prop_view))
        end                                                             as property_view,
        case
            when upper(trim(s.corner_unit)) = 'YES' then true
            else false
        end                                                             as is_corner_unit,

        -- ── Derived metrics (computed from raw columns) ──
        -- Estimated rent: tiered GRM — lower rate for higher-value properties
        case
            when try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) > 2000000
            then round(try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) * 0.0025, 2)
            when try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) > 1000000
            then round(try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) * 0.0030, 2)
            when try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) > 700000
            then round(try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) * 0.0040, 2)
            when try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) > 400000
            then round(try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) * 0.0050, 2)
            when try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) > 0
            then round(try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) * 0.0060, 2)
            else -999.0
        end                                                             as estimated_rent,

        -- Price per sqft: total value / living area
        case
            when try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) > 0
                and try_cast(nullif(trim(s.living_area), '') as float) > 0
            then round(
                try_cast(replace(replace(nullif(trim(s.total_value), ''), ',', ''), chr(36), '') as float) /
                try_cast(nullif(trim(s.living_area), '') as float),
                2
            )
            else -999.0
        end                                                             as price_per_sqft,

        -- Property age: current year minus year built
        case
            when nullif(trim(s.yr_built), '') is not null
                and try_cast(nullif(trim(s.yr_built), '') as integer) > 0
            then (2026 - try_cast(nullif(trim(s.yr_built), '') as integer))
            else -99
        end                                                             as property_age,

        -- Renovation recency score: bucketed by decade of last remodel (1=oldest, 9=most recent)
        case
            when nullif(trim(s.yr_remodel), '') is null then -99
            when try_cast(nullif(trim(s.yr_remodel), '') as integer) >= 2020 then 9
            when try_cast(nullif(trim(s.yr_remodel), '') as integer) >= 2015 then 8
            when try_cast(nullif(trim(s.yr_remodel), '') as integer) >= 2010 then 7
            when try_cast(nullif(trim(s.yr_remodel), '') as integer) >= 2005 then 6
            when try_cast(nullif(trim(s.yr_remodel), '') as integer) >= 2000 then 5
            when try_cast(nullif(trim(s.yr_remodel), '') as integer) >= 1995 then 4
            when try_cast(nullif(trim(s.yr_remodel), '') as integer) >= 1990 then 3
            when try_cast(nullif(trim(s.yr_remodel), '') as integer) >= 1980 then 2
            when try_cast(nullif(trim(s.yr_remodel), '') as integer) >  0    then 1
            else -99
        end                                                             as renovation_recency_score,

        -- Parking flag: true if num_parking > 0
        case
            when try_cast(s.num_parking as integer) > 0 then true
            else false
        end                                                             as is_parking,

        -- ── Geocoordinates ────────────────────
        coalesce(g.lat,  -999.0)                                        as lat,
        coalesce(g.long, -999.0)                                        as long,
        case
            when g.lat is not null then true
            else false
        end                                                             as has_valid_location

    from source s
    inner join geocoded g
        on trim(s.pid) = g.pid
    where s.pid is not null
),

-- ─────────────────────────────────────────────
-- STEP 2: Derived flags and business logic
-- ─────────────────────────────────────────────
final as (
    select
        *,

        -- Property type flags
        case
            when land_use_code in ('CD', 'CC', 'CP') then true
            else false
        end                                                             as is_condo,

        case
            when land_use_code in ('A','C','CL','CM') then true
            else false
        end                                                             as is_rental_property,

        case
            when land_use_code in ('E','I','R1') then false
            else true
        end                                                             as is_residential,

        -- Amenity flags
        case
            when ac_type in ('CENTRAL AC', 'DUCTLESS AC') then true
            else false
        end                                                             as has_ac,

        case
            when num_parking > 0 then true
            else false
        end                                                             as has_parking_space,

        case
            when fireplaces > 0 then true
            else false
        end                                                             as has_fireplace,

        -- Renovation flag (renovated in last 15 years relative to data)
        case
            when year_remodel >= 2010 then true
            else false
        end                                                             as is_recently_renovated,

        -- Condition score (numeric mapping for analytics)
        case
            when overall_condition = 'EXCELLENT'  then 5
            when overall_condition = 'VERY GOOD'  then 4
            when overall_condition = 'GOOD'        then 3
            when overall_condition = 'AVERAGE'     then 2
            when overall_condition = 'FAIR'        then 1
            when overall_condition = 'POOR'        then 0
            else -99
        end                                                             as condition_score,

        -- Metadata
        current_timestamp()                                             as dbt_loaded_at

    from casted
)

select * from final