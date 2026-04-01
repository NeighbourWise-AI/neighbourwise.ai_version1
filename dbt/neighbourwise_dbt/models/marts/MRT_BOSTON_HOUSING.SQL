{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

/*
================================================================================
MRT_BOSTON_HOUSING
--------------------------------------------------------------------------------
Unified property-level housing mart combining three sources:
  BOSTON        → INT_BOSTON_HOUSING       (City of Boston assessor)
  CAMBRIDGE     → INT_CAMBRIDGE_HOUSING    (City of Cambridge assessor)
  GREATER_BOSTON→ INT_GREATER_BOSTON_HOUSING (MassGIS L3: Quincy, Malden,
                                             Medford, Arlington, Watertown, Salem)

All three branches are spatially joined to STG_MASTER_LOCATION via
ST_CONTAINS to assign location_id and neighborhood_name.

Column alignment across all three branches — 37 output columns:
  property_id, data_source, full_address, street_num, street_name,
  unit_num, zip_code, property_class, building_type, owner_name,
  gross_area_sqft, living_area_sqft, land_sqft, land_value, building_value,
  total_assessed_value, estimated_rent, price_per_sqft, year_built,
  property_age, bedrooms, full_baths, fireplaces, overall_condition,
  condition_score, is_condo, is_rental_property, is_residential,
  has_ac, has_parking_space, has_fireplace, is_parking,
  lat, long, has_valid_location, location_id, neighborhood_name,
  city, resolution_method, row_description, load_timestamp

Columns absent in a source are NULL-filled for that branch.
================================================================================
*/

with

master_location as (
    select
        location_id,
        name         as neighborhood_name,
        city,
        geometry_wkt as geometry
    from {{ source('STAGE', 'STG_MASTER_LOCATION') }}
    where geometry_wkt is not null
),

-- ─────────────────────────────────────────────────────────────────────────────
-- BOSTON branch
-- ─────────────────────────────────────────────────────────────────────────────
boston_geo as (
    select
        b.*,
        ml.location_id       as location_id,
        ml.neighborhood_name as neighborhood_name,
        ml.city              as resolved_city
    from {{ ref('INT_BOSTON_HOUSING') }} b
    inner join master_location ml
        on b.has_valid_location = true
        and st_contains(
            to_geography(ml.geometry),
            to_geography(st_makepoint(b.long, b.lat))
        )
),

boston_final as (
    select
        property_id,
        'BOSTON'                                                        as data_source,
        street_num || ' ' || street_name                                as full_address,
        street_num,
        street_name,
        unit_num,
        zip_code,
        land_use_desc                                                   as property_class,
        building_type,
        owner_name,
        gross_area_sqft,
        living_area_sqft,
        land_sqft,
        land_value,
        building_value,
        total_assessed_value,
        estimated_rent,
        price_per_sqft,
        year_built,
        property_age,
        bedrooms,
        full_baths,
        fireplaces,
        overall_condition,
        condition_score,
        is_condo,
        is_rental_property,
        is_residential,
        has_ac,
        has_parking_space,
        has_fireplace,
        is_parking,
        lat,
        long,
        has_valid_location,
        location_id,
        neighborhood_name,
        resolved_city                                                   as city,
        'SPATIAL'                                                       as resolution_method,
        'Property at ' || street_num || ' ' || street_name ||
        case when unit_num != 'UNKNOWN' then ' Unit ' || unit_num else '' end ||
        ' in ' || neighborhood_name || ', ' || resolved_city ||
        ' (' || zip_code || ').' ||
        ' It is a ' || lower(land_use_desc) ||
        case when building_type != 'UNKNOWN'
             then ' (' || lower(building_type) || ')' else '' end || '.' ||
        case when bedrooms > 0
             then ' The property has ' || bedrooms || ' bedroom(s) and '
                  || full_baths || ' full bath(s).' else '' end ||
        case when living_area_sqft > 0
             then ' Living area is ' || living_area_sqft || ' sq ft.' else '' end ||
        case when total_assessed_value > 0
             then ' Total assessed value is $'
                  || to_char(total_assessed_value, '999,999,999') || '.' else '' end ||
        case when estimated_rent > 0
             then ' Estimated monthly rent is $'
                  || to_char(estimated_rent, '999,999') || '.' else '' end ||
        ' Built in ' || year_built || '.' ||
        ' Overall condition is ' || lower(overall_condition) || '.' ||
        case when has_ac           then ' Has air conditioning.' else '' end ||
        case when has_fireplace    then ' Has fireplace(s).'     else '' end ||
        case when has_parking_space then ' Parking available.'   else '' end
                                                                        as row_description,
        to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS')            as load_timestamp
    from boston_geo
    where property_id is not null
      and has_valid_location = true
),

-- ─────────────────────────────────────────────────────────────────────────────
-- CAMBRIDGE branch
-- ─────────────────────────────────────────────────────────────────────────────
cambridge_geo as (
    select
        c.*,
        ml.location_id       as location_id,
        ml.neighborhood_name as neighborhood_name,
        ml.city              as resolved_city
    from {{ ref('INT_CAMBRIDGE_HOUSING') }} c
    inner join master_location ml
        on c.has_valid_location = true
        and st_contains(
            to_geography(ml.geometry),
            to_geography(st_makepoint(c.long, c.lat))
        )
),

cambridge_final as (
    select
        property_id,
        'CAMBRIDGE'                                                     as data_source,
        full_address,
        -- Parse street_num from full_address: first space-delimited token
        case
            when full_address = 'UNKNOWN' then null
            else split_part(full_address, ' ', 1)
        end                                                             as street_num,
        -- Parse street_name: everything after the first space-delimited token
        case
            when full_address = 'UNKNOWN' then null
            when charindex(' ', full_address) > 0
            then trim(substr(full_address, charindex(' ', full_address) + 1))
            else null
        end                                                             as street_name,
        unit_num,
        zip_code,
        property_class,
        building_type,
        owner_name,
        null::integer                                                   as gross_area_sqft,
        living_area_sqft,
        land_sqft,
        land_value,
        building_value,
        total_assessed_value,
        estimated_rent,
        price_per_sqft,
        year_built,
        property_age,
        bedrooms,
        full_baths,
        fireplaces,
        overall_condition,
        condition_score,
        is_condo,
        is_rental_property,
        is_residential,
        has_ac,
        has_parking_space,
        has_fireplace,
        is_parking,
        lat,
        long,
        has_valid_location,
        location_id,
        neighborhood_name,
        resolved_city                                                   as city,
        'SPATIAL'                                                       as resolution_method,
        'Property at ' || full_address ||
        case when unit_num != 'UNKNOWN' then ' Unit ' || unit_num else '' end ||
        ' in ' || neighborhood_name || ', ' || resolved_city ||
        ' (' || zip_code || ').' ||
        ' It is a ' || lower(property_class) ||
        case when building_type != 'UNKNOWN'
             then ' (' || lower(building_type) || ')' else '' end || '.' ||
        case when bedrooms > 0
             then ' The property has ' || bedrooms || ' bedroom(s) and '
                  || full_baths || ' full bath(s).' else '' end ||
        case when living_area_sqft > 0
             then ' Living area is ' || living_area_sqft || ' sq ft.' else '' end ||
        case when total_assessed_value > 0
             then ' Total assessed value is $'
                  || to_char(total_assessed_value, '999,999,999') || '.' else '' end ||
        case when estimated_rent > 0
             then ' Estimated monthly rent is $'
                  || to_char(estimated_rent, '999,999') || '.' else '' end ||
        ' Built in ' || year_built || '.' ||
        ' Overall condition is ' || lower(overall_condition) || '.' ||
        case when has_ac           then ' Has air conditioning.' else '' end ||
        case when has_fireplace    then ' Has fireplace(s).'     else '' end ||
        case when has_parking_space then ' Parking available.'   else '' end
                                                                        as row_description,
        to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS')            as load_timestamp
    from cambridge_geo
    where property_id is not null
      and has_valid_location = true
),

-- ─────────────────────────────────────────────────────────────────────────────
-- GREATER BOSTON branch
-- Quincy (84), Malden (147), Medford (148), Arlington (5),
-- Watertown (53), Salem (223)
-- ─────────────────────────────────────────────────────────────────────────────
greater_boston_geo as (
    select
        g.*,
        ml.location_id       as location_id,
        ml.neighborhood_name as neighborhood_name,
        ml.city              as resolved_city
    from {{ ref('INT_GREATER_BOSTON_HOUSING') }} g
    inner join master_location ml
        on g.has_valid_location = true
        and st_contains(
            to_geography(ml.geometry),
            to_geography(st_makepoint(g.long, g.lat))
        )
),

greater_boston_final as (
    select
        property_id,
        'GREATER_BOSTON'                                                as data_source,
        full_address,
        street_num,
        street_name,
        unit_num,
        zip_code,
        property_class,
        building_type,
        owner_name,
        gross_area_sqft,
        living_area_sqft,
        land_sqft,
        land_value,
        building_value,
        total_assessed_value,
        estimated_rent,
        price_per_sqft,
        year_built,
        property_age,
        null::integer                                                   as bedrooms,
        null::integer                                                   as full_baths,
        null::integer                                                   as fireplaces,
        overall_condition,
        condition_score,
        is_condo,
        is_rental_property,
        is_residential,
        has_ac,
        has_parking_space,
        has_fireplace,
        is_parking,
        lat,
        long,
        has_valid_location,
        location_id,
        neighborhood_name,
        resolved_city                                                   as city,
        'SPATIAL'                                                       as resolution_method,
        'Property at ' || full_address ||
        ' in ' || neighborhood_name || ', ' || resolved_city ||
        ' (' || zip_code || ').' ||
        ' It is a ' || lower(property_class) ||
        case when building_type != 'UNKNOWN'
             then ' (' || lower(building_type) || ')' else '' end || '.' ||
        case when living_area_sqft > 0
             then ' Living area is ' || living_area_sqft || ' sq ft.' else '' end ||
        case when total_assessed_value > 0
             then ' Total assessed value is $'
                  || to_char(total_assessed_value, '999,999,999') || '.' else '' end ||
        case when estimated_rent > 0
             then ' Estimated monthly rent is $'
                  || to_char(estimated_rent, '999,999') || '.' else '' end ||
        ' Built in ' || year_built || '.'
                                                                        as row_description,
        to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS')            as load_timestamp
    from greater_boston_geo
    where property_id is not null
      and has_valid_location = true
)

-- ─────────────────────────────────────────────────────────────────────────────
-- UNION ALL — Boston + Cambridge + Greater Boston
-- ─────────────────────────────────────────────────────────────────────────────
select * from boston_final
union all
select * from cambridge_final
union all
select * from greater_boston_final