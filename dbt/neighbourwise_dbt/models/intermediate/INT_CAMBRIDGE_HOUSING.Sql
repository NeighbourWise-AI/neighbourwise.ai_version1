{{
    config(
        materialized = 'table',
        schema       = 'INTERMEDIATE',
        tags         = ['housing', 'cambridge', 'intermediate']
    )
}}

/*
================================================================================
INT_CAMBRIDGE_HOUSING
--------------------------------------------------------------------------------
Source : STAGE.STG_CAMBRIDGE_HOUSING
Layer  : Intermediate
--------------------------------------------------------------------------------
Column names reference the RAW API columns as loaded by cambridge_housing_load.py:
  PID, GISID, ADDRESS, UNIT, LATITUDE, LONGITUDE,
  STATECLASSCODE, PROPERTYCLASS, ZONING, LANDAREA,
  YEAROFASSESSMENT, BUILDINGVALUE, LANDVALUE, ASSESSEDVALUE,
  SALEPRICE, OWNER_NAME, OWNER_ZIP, OWNER_CITY,
  EXTERIOR_STYLE, EXTERIOR_WALLTYPE, EXTERIOR_ROOFTYPE, EXTERIOR_ROOFMATERIAL,
  EXTERIOR_FLOORLOCATION, EXTERIOR_VIEW,
  INTERIOR_LIVINGAREA, INTERIOR_NUMUNITS, INTERIOR_TOTALROOMS,
  INTERIOR_BEDROOMS, INTERIOR_KITCHENS, INTERIOR_FULLBATHS,
  INTERIOR_HALFBATHS, INTERIOR_FIREPLACES, INTERIOR_FLOORING,
  SYSTEMS_HEATTYPE, SYSTEMS_HEATFUEL, SYSTEMS_CENTRALAIR,
  CONDITION_YEARBUILT, CONDITION_INTERIORCONDITION,
  CONDITION_OVERALLCONDITION, CONDITION_OVERALLGRADE,
  PARKING_OPEN, PARKING_COVERED, PARKING_GARAGE,
  UNFINISHEDBASEMENTGROSS, FINISHEDBASEMENTGROSS

Key schema differences from Boston housing:
  - LAT/LONG come from API directly — no geocode join needed
  - AC stored as 'True'/'False' boolean string (SYSTEMS_CENTRALAIR)
  - Condition values are plain words — no coded prefix to strip
  - Currency values are plain numerics — no CHR(36) stripping needed
  - Property class is descriptive text (CONDOMINIUM, TWO FAMILY etc.)
================================================================================
*/

with

source as (
    select * from {{ source('STAGE', 'STG_CAMBRIDGE_HOUSING') }}
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Cast, clean, sentinel-fill using raw STG column names
-- ─────────────────────────────────────────────────────────────────────────────
casted as (
    select
        -- ── Keys ──────────────────────────────
        coalesce(nullif(trim(s.PID),   ''), 'UNKNOWN')                  as property_id,
        coalesce(nullif(trim(s.GISID), ''), 'UNKNOWN')                  as gis_id,

        -- ── Address ───────────────────────────
        coalesce(nullif(trim(upper(s.ADDRESS)), ''), 'UNKNOWN')         as full_address,
        coalesce(nullif(trim(upper(s.UNIT)),    ''), 'UNKNOWN')         as unit_num,
        'CAMBRIDGE'                                                     as city,
        case
            when regexp_like(trim(s.OWNER_ZIP), '^[0-9]{5}$')
            then trim(s.OWNER_ZIP)
            else 'UNKNOWN'
        end                                                             as zip_code,

        -- ── Property classification ───────────
        coalesce(nullif(trim(upper(s.PROPERTYCLASS)),   ''), 'UNKNOWN') as property_class,
        coalesce(nullif(trim(upper(s.STATECLASSCODE)),  ''), 'UNKNOWN') as state_class_code,
        coalesce(nullif(trim(upper(s.ZONING)),          ''), 'UNKNOWN') as zoning,
        coalesce(nullif(trim(upper(s.EXTERIOR_STYLE)),  ''), 'UNKNOWN') as building_type,

        -- ── Ownership ─────────────────────────
        coalesce(nullif(trim(upper(s.OWNER_NAME)), ''), 'UNKNOWN')      as owner_name,

        -- ── Building metrics ──────────────────
        coalesce(try_cast(s.LANDAREA               as integer), -99)    as land_sqft,
        coalesce(try_cast(s.INTERIOR_LIVINGAREA    as integer), -99)    as living_area_sqft,
        coalesce(try_cast(s.INTERIOR_NUMUNITS      as integer), -99)    as residential_units,
        coalesce(try_cast(s.INTERIOR_TOTALROOMS    as integer), -99)    as total_rooms,
        coalesce(try_cast(s.EXTERIOR_NUMSTORIES    as float),   -99)    as num_stories,

        -- ── Valuation (plain numerics — no $ stripping needed) ────────────
        coalesce(try_cast(nullif(trim(s.LANDVALUE),     '') as float), -999.0) as land_value,
        coalesce(try_cast(nullif(trim(s.BUILDINGVALUE), '') as float), -999.0) as building_value,
        coalesce(try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float), -999.0) as total_assessed_value,
        coalesce(try_cast(nullif(trim(s.SALEPRICE),     '') as float), -999.0) as sale_price,
        coalesce(try_cast(nullif(trim(s.YEAROFASSESSMENT), '') as integer), -99) as assessment_year,

        -- ── Property characteristics ──────────
        coalesce(try_cast(nullif(trim(s.CONDITION_YEARBUILT),  '') as integer), -99) as year_built,
        coalesce(try_cast(s.INTERIOR_BEDROOMS  as integer), -99)        as bedrooms,
        coalesce(try_cast(s.INTERIOR_FULLBATHS as integer), -99)        as full_baths,
        coalesce(try_cast(s.INTERIOR_HALFBATHS as integer), -99)        as half_baths,
        coalesce(try_cast(s.INTERIOR_KITCHENS  as integer), -99)        as kitchens,
        coalesce(try_cast(s.INTERIOR_FIREPLACES as integer), -99)       as fireplaces,

        -- ── Physical attributes ───────────────
        coalesce(nullif(trim(upper(s.EXTERIOR_ROOFTYPE)),     ''), 'UNKNOWN') as roof_structure,
        coalesce(nullif(trim(upper(s.EXTERIOR_ROOFMATERIAL)), ''), 'UNKNOWN') as roof_cover,
        coalesce(nullif(trim(upper(s.EXTERIOR_WALLTYPE)),     ''), 'UNKNOWN') as exterior_finish,
        coalesce(nullif(trim(upper(s.INTERIOR_FLOORING)),     ''), 'UNKNOWN') as interior_wall,
        coalesce(nullif(trim(upper(s.SYSTEMS_HEATTYPE)),      ''), 'UNKNOWN') as heat_type,
        coalesce(nullif(trim(upper(s.SYSTEMS_HEATFUEL)),      ''), 'UNKNOWN') as heat_fuel,
        coalesce(nullif(trim(upper(s.EXTERIOR_VIEW)),         ''), 'UNKNOWN') as property_view,
        coalesce(nullif(trim(upper(s.EXTERIOR_FLOORLOCATION)),''), 'UNKNOWN') as floor_location,

        -- AC: Cambridge stores as boolean string 'True'/'False'
        case
            when upper(trim(s.SYSTEMS_CENTRALAIR)) in ('TRUE', 'YES', '1') then 'CENTRAL AC'
            when upper(trim(s.SYSTEMS_CENTRALAIR)) in ('FALSE','NO',  '0') then 'NONE'
            else 'UNKNOWN'
        end                                                             as ac_type,

        -- ── Condition (plain words — no coded prefix to strip) ─────────────
        coalesce(nullif(trim(upper(s.CONDITION_OVERALLCONDITION)), ''), 'UNKNOWN') as overall_condition,
        coalesce(nullif(trim(upper(s.CONDITION_INTERIORCONDITION)),''), 'UNKNOWN') as interior_condition,
        coalesce(nullif(trim(upper(s.CONDITION_OVERALLGRADE)),     ''), 'UNKNOWN') as overall_grade,

        -- ── Parking ───────────────────────────
        coalesce(try_cast(s.PARKING_OPEN    as integer), 0)             as parking_open,
        coalesce(try_cast(s.PARKING_COVERED as integer), 0)             as parking_covered,
        coalesce(try_cast(s.PARKING_GARAGE  as integer), 0)             as parking_garage,

        -- ── Derived metrics ───────────────────
        -- Estimated rent: tiered GRM — lower rate for higher-value properties
        case
            when try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) > 2000000
            then round(try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) * 0.0025, 2)
            when try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) > 1000000
            then round(try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) * 0.0030, 2)
            when try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) > 700000
            then round(try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) * 0.0040, 2)
            when try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) > 400000
            then round(try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) * 0.0050, 2)
            when try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) > 0
            then round(try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) * 0.0060, 2)
            else -999.0
        end                                                             as estimated_rent,

        -- Price per sqft
        case
            when try_cast(nullif(trim(s.INTERIOR_LIVINGAREA), '') as float) > 0
                and try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) > 0
            then round(
                try_cast(nullif(trim(s.ASSESSEDVALUE), '') as float) /
                try_cast(nullif(trim(s.INTERIOR_LIVINGAREA), '') as float),
                2
            )
            else -999.0
        end                                                             as price_per_sqft,

        -- Property age
        case
            when try_cast(nullif(trim(s.CONDITION_YEARBUILT), '') as integer) > 0
            then (2026 - try_cast(nullif(trim(s.CONDITION_YEARBUILT), '') as integer))
            else -99
        end                                                             as property_age,

        -- Total parking spaces
        coalesce(try_cast(s.PARKING_OPEN    as integer), 0) +
        coalesce(try_cast(s.PARKING_COVERED as integer), 0) +
        coalesce(try_cast(s.PARKING_GARAGE  as integer), 0)            as num_parking,

        -- ── Coordinates (directly from API — no geocoder needed) ──────────
        coalesce(try_cast(nullif(trim(s.LATITUDE),  '') as float), -999.0) as lat,
        coalesce(try_cast(nullif(trim(s.LONGITUDE), '') as float), -999.0) as long,
        case
            when try_cast(nullif(trim(s.LATITUDE), '') as float) is not null
                and try_cast(nullif(trim(s.LATITUDE), '') as float) != 0
            then true
            else false
        end                                                             as has_valid_location

    from source s
    where s.PID is not null
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Derived flags
-- ─────────────────────────────────────────────────────────────────────────────
final as (
    select
        *,

        -- Property type flags
        case
            when property_class ilike '%CONDO%'       then true
            else false
        end                                                             as is_condo,

        case
            when property_class ilike '%TWO%'
              or property_class ilike '%THREE%'
              or property_class ilike '%APT%'
              or property_class ilike '%MULTI%'
              or property_class ilike '%FOUR%'
              or property_class ilike '%FIVE%'
              or property_class ilike '%SIX%'
            then true
            else false
        end                                                             as is_rental_property,

        case
            when property_class ilike '%COMMERCIAL%'
              or property_class ilike '%OFFICE%'
              or property_class ilike '%RETAIL%'
              or property_class ilike '%INDUSTRIAL%'
              or property_class ilike '%PARKING%'
            then false
            else true
        end                                                             as is_residential,

        -- Amenity flags
        case when ac_type = 'CENTRAL AC'  then true else false end      as has_ac,
        case when num_parking > 0         then true else false end      as has_parking_space,
        case when fireplaces > 0          then true else false end      as has_fireplace,

        -- Parking flag
        case
            when parking_open > 0
              or parking_covered > 0
              or parking_garage > 0
            then true else false
        end                                                             as is_parking,

        -- Condition score
        case
            when overall_condition = 'EXCELLENT'  then 5
            when overall_condition = 'VERY GOOD'  then 4
            when overall_condition = 'GOOD'        then 3
            when overall_condition = 'AVERAGE'     then 2
            when overall_condition = 'FAIR'        then 1
            when overall_condition = 'POOR'        then 0
            else -99
        end                                                             as condition_score,

        -- Source tag — useful when unioning Boston + Cambridge data
        'CAMBRIDGE'                                                     as data_source,

        current_timestamp()                                             as dbt_loaded_at

    from casted
)

select * from final