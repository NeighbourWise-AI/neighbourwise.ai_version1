{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

with int_grocery as (
    select * from {{ ref('INT_BOSTON_GROCERY_STORES') }}
),

master_location as (
    select
        location_id,
        name         as neighborhood_name,
        city,
        geometry_wkt as geometry
    from {{ source('STAGE', 'STG_MASTER_LOCATION') }}
    where geometry_wkt is not null
),

-- ─────────────────────────────────────────────
-- Spatial join — assign neighborhood via ST_CONTAINS
-- Falls back to city from the intermediate layer if no spatial match
-- ─────────────────────────────────────────────
with_neighborhood as (
    select
        g.*,
        ml.location_id                                              as location_id,
        coalesce(ml.neighborhood_name, g.city)                     as neighborhood_name,
        coalesce(ml.city, g.city)                                   as resolved_city,
        case
            when ml.neighborhood_name is not null then 'SPATIAL'
            else 'CITY'
        end                                                         as resolution_method
    from int_grocery g
    left join master_location ml
        on g.has_valid_location = true
        and st_contains(
            to_geography(ml.geometry),
            to_geography(st_makepoint(
                case when g.has_valid_location = true then g.long else 0 end,
                case when g.has_valid_location = true then g.lat  else 0 end
            ))
        )
),

-- ─────────────────────────────────────────────
-- SQL-templated row description
-- Used for vector embeddings and LLM context injection
-- ─────────────────────────────────────────────
with_description as (
    select
        -- ── Keys ──────────────────────────────
        objectid,

        -- ── Store identity ────────────────────
        store_name,
        street_address,
        zip_code,
        data_year,
        data_vintage,

        -- ── Classification ────────────────────
        store_type,
        naics_category,

        -- ── Flags ─────────────────────────────
        is_essential_food_source,
        is_pharmacy_or_drug_store,
        is_specialty_store,
        is_large_format,
        is_convenience_type,
        is_zip_valid,

        -- ── Coordinates ───────────────────────
        lat,
        long,
        has_valid_location,

        -- ── Geographic resolution ─────────────
        location_id,
        neighborhood_name,
        resolved_city                                               as city,
        resolution_method,

        -- ── Natural language description ───────
        -- Used for RAG vector embeddings and LLM context injection
        store_name || ' is a ' ||
        lower(replace(store_type, '_', ' ')) ||
        ' located at ' || street_address ||
        ', ' || neighborhood_name || ', ' || resolved_city ||
        ' ' || zip_code || '.' ||
        ' It is classified under ' || naics_category || '.' ||
        case
            when is_essential_food_source
            then ' It is considered an essential food source for the neighborhood.'
            else ''
        end ||
        case
            when is_large_format
            then ' It is a large-format retail store.'
            else ''
        end ||
        case
            when is_specialty_store
            then ' It is a specialty food store.'
            else ''
        end ||
        case
            when is_pharmacy_or_drug_store
            then ' It also provides pharmacy or drug store services.'
            else ''
        end ||
        case
            when is_convenience_type
            then ' It is a convenience-type store.'
            else ''
        end ||
        ' Data sourced from the ' || data_vintage || ' Massachusetts grocery licensing survey.'
                                                                    as row_description,

        -- ── Metadata ──────────────────────────
        to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS')        as load_timestamp

    from with_neighborhood
)

select *
from with_description
where objectid is not null
  and has_valid_location = true