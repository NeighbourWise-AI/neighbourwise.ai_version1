{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

/*
================================================================================
MRT_NEIGHBORHOOD_HOUSING
--------------------------------------------------------------------------------
Neighbourhood-level housing intelligence scored on value for money.

SCORING — two signals, each min-max normalised within peer group:

  Signal 1 — Affordability (60 pts)
    avg_price_per_sqft INVERTED — cheapest $/sqft scores 60, most expensive 0
    Reflects how affordable the neighbourhood is relative to its peers

  Signal 2 — Space (40 pts)
    avg_living_area_sqft DIRECT — largest avg sqft scores 40, smallest 0
    Reflects how much space residents typically get

  Each neighbourhood is scored against its own peer group only:
    BOSTON         — 26 Boston neighbourhoods
    CAMBRIDGE      — 13 Cambridge neighbourhoods
    GREATER_BOSTON — 11 Greater Boston suburbs

  Pass 2 re-normalises all groups to a shared 0–100 scale centred on 50
  so Boston, Cambridge and Greater Boston scores are directly comparable.

  Result: Hyde Park (cheap, large) scores high; Back Bay (expensive, small)
  scores low. This answers: "where do I get the most space for my money?"
================================================================================
*/

with

-- ── Housing aggregation ───────────────────────────────────────────────────
housing_agg as (
    select
        ml.location_id,
        ml.name                                                             as neighborhood_name,
        ml.city,
        ml.sqmiles,

        count(h.property_id)                                                as total_properties,
        sum(case when h.data_source = 'BOSTON'         then 1 else 0 end)  as boston_count,
        sum(case when h.data_source = 'CAMBRIDGE'      then 1 else 0 end)  as cambridge_count,
        sum(case when h.data_source = 'GREATER_BOSTON' then 1 else 0 end)  as greater_boston_count,

        sum(case when h.is_condo           then 1 else 0 end)              as condo_count,
        sum(case when h.is_rental_property then 1 else 0 end)              as rental_count,
        sum(case when h.is_residential     then 1 else 0 end)              as residential_count,

        round(avg(case when h.living_area_sqft > 0
                       then h.living_area_sqft end), 0)                    as avg_living_area_sqft,
        round(avg(case when h.bedrooms > 0
                       then h.bedrooms end), 1)                            as avg_bedrooms,
        round(avg(case when h.full_baths > 0
                       then h.full_baths end), 1)                          as avg_full_baths,

        round(avg(case when h.total_assessed_value > 0
                       then h.total_assessed_value end), 0)                as avg_assessed_value,
        round(avg(case when h.estimated_rent > 0
                       then h.estimated_rent end), 0)                      as avg_estimated_rent,
        round(avg(case when h.price_per_sqft > 0
                       then h.price_per_sqft end), 2)                      as avg_price_per_sqft,
        round(min(case when h.total_assessed_value > 0
                       then h.total_assessed_value end), 0)                as min_assessed_value,
        round(max(case when h.total_assessed_value > 0
                       then h.total_assessed_value end), 0)                as max_assessed_value,

        sum(case when h.overall_condition = 'EXCELLENT' then 1 else 0 end) as excellent_count,
        sum(case when h.overall_condition = 'VERY GOOD' then 1 else 0 end) as very_good_count,
        sum(case when h.overall_condition = 'GOOD'      then 1 else 0 end) as good_count,
        sum(case when h.overall_condition = 'AVERAGE'   then 1 else 0 end) as average_count,
        sum(case when h.overall_condition = 'FAIR'      then 1 else 0 end) as fair_count,
        round(avg(case when h.condition_score >= 0
                       then h.condition_score end), 2)                     as avg_condition_score,

        sum(case when h.has_ac            then 1 else 0 end)               as has_ac_count,
        sum(case when h.has_parking_space then 1 else 0 end)               as has_parking_count,
        sum(case when h.has_fireplace     then 1 else 0 end)               as has_fireplace_count,

        round(avg(case when h.property_age > 0
                       then h.property_age end), 0)                        as avg_property_age,
        min(case when h.year_built > 0 then h.year_built end)              as oldest_year_built,
        max(case when h.year_built > 0 then h.year_built end)              as newest_year_built

    from {{ source('STAGE', 'STG_MASTER_LOCATION') }} ml
    left join {{ ref('MRT_BOSTON_HOUSING') }} h
        on h.location_id = ml.location_id
        and h.is_residential = true
        and (h.living_area_sqft    <= 0 or h.living_area_sqft    < 10000)
        and (h.total_assessed_value <= 0 or h.total_assessed_value < 10000000)
    group by
        ml.location_id,
        ml.name,
        ml.city,
        ml.sqmiles
),

-- ── Derived metrics ───────────────────────────────────────────────────────
with_metrics as (
    select
        *,

        case when sqmiles > 0 then round(total_properties / sqmiles, 2) else 0
        end                                                                 as properties_per_sqmile,

        -- Condition % excludes UNKNOWN rows from denominator
        case when (excellent_count + very_good_count + good_count +
                   average_count + fair_count) > 0
            then round(
                (excellent_count + very_good_count + good_count) * 100.0 /
                nullif(excellent_count + very_good_count + good_count +
                       average_count  + fair_count, 0), 1)
            else 0
        end                                                                 as pct_good_condition,

        case when total_properties > 0
            then round(has_ac_count * 100.0 / total_properties, 1) else 0
        end                                                                 as pct_has_ac,

        case when total_properties > 0
            then round(has_parking_count * 100.0 / total_properties, 1) else 0
        end                                                                 as pct_has_parking,

        round((
            coalesce(has_ac_count      * 100.0 / nullif(total_properties, 0), 0) +
            coalesce(has_parking_count * 100.0 / nullif(total_properties, 0), 0)
        ) / 2.0, 1)                                                        as amenity_rate,

        case when total_properties < 50 then true else false end           as insufficient_data,

        case
            when city = 'BOSTON'    then 'BOSTON'
            when city = 'CAMBRIDGE' then 'CAMBRIDGE'
            else 'GREATER_BOSTON'
        end                                                                 as distribution_group

    from housing_agg
),

-- ── PASS 1: Min-max distributions within each peer group ─────────────────
boston_dist as (
    select
        min(avg_price_per_sqft)   as min_ppsf,  max(avg_price_per_sqft)   as max_ppsf,
        min(avg_living_area_sqft) as min_sqft,  max(avg_living_area_sqft) as max_sqft
    from with_metrics
    where distribution_group = 'BOSTON'
      and insufficient_data = false
      and avg_price_per_sqft   > 0
      and avg_living_area_sqft > 0
),

cambridge_dist as (
    select
        min(avg_price_per_sqft)   as min_ppsf,  max(avg_price_per_sqft)   as max_ppsf,
        min(avg_living_area_sqft) as min_sqft,  max(avg_living_area_sqft) as max_sqft
    from with_metrics
    where distribution_group = 'CAMBRIDGE'
      and insufficient_data = false
      and avg_price_per_sqft   > 0
      and avg_living_area_sqft > 0
),

greater_boston_dist as (
    select
        min(avg_price_per_sqft)   as min_ppsf,  max(avg_price_per_sqft)   as max_ppsf,
        min(avg_living_area_sqft) as min_sqft,  max(avg_living_area_sqft) as max_sqft
    from with_metrics
    where distribution_group = 'GREATER_BOSTON'
      and insufficient_data = false
      and avg_price_per_sqft   > 0
      and avg_living_area_sqft > 0
),

-- ── Pass 1 score: affordability (inverted ppsf 60%) + space (sqft 40%) ───
with_pass1_scores as (
    select
        m.*,
        case
            when m.insufficient_data    = true then null
            when m.avg_price_per_sqft  <= 0    then null
            when m.avg_living_area_sqft <= 0   then null

            when m.distribution_group = 'BOSTON' then
                round(least(100, greatest(0,
                    -- affordability: cheapest $/sqft → highest score (inverted)
                    60.0 * (d_b.max_ppsf - m.avg_price_per_sqft)   / nullif(d_b.max_ppsf - d_b.min_ppsf, 0)
                    -- space: largest sqft → highest score (direct)
                  + 40.0 * (m.avg_living_area_sqft - d_b.min_sqft) / nullif(d_b.max_sqft - d_b.min_sqft, 0)
                )), 1)

            when m.distribution_group = 'CAMBRIDGE' then
                round(least(100, greatest(0,
                    60.0 * (d_c.max_ppsf - m.avg_price_per_sqft)   / nullif(d_c.max_ppsf - d_c.min_ppsf, 0)
                  + 40.0 * (m.avg_living_area_sqft - d_c.min_sqft) / nullif(d_c.max_sqft - d_c.min_sqft, 0)
                )), 1)

            when m.distribution_group = 'GREATER_BOSTON' then
                round(least(100, greatest(0,
                    60.0 * (d_gb.max_ppsf - m.avg_price_per_sqft)   / nullif(d_gb.max_ppsf - d_gb.min_ppsf, 0)
                  + 40.0 * (m.avg_living_area_sqft - d_gb.min_sqft) / nullif(d_gb.max_sqft - d_gb.min_sqft, 0)
                )), 1)

            else null
        end                                                                 as pass1_score

    from with_metrics m
    cross join boston_dist         d_b
    cross join cambridge_dist      d_c
    cross join greater_boston_dist d_gb
),

-- ── PASS 2: Re-normalise to shared 0-100 scale ───────────────────────────
pass2_dist as (
    select
        avg(pass1_score)    as mean_score,
        stddev(pass1_score) as std_score
    from with_pass1_scores
    where insufficient_data = false
),

with_final_score as (
    select
        p.*,
        case
            when p.insufficient_data = true then null
            else round(least(100, greatest(0,
                50.0 + ((p.pass1_score - d2.mean_score) / nullif(d2.std_score, 0)) * 15.0
            )), 1)
        end                                                                 as housing_score
    from with_pass1_scores p
    cross join pass2_dist d2
),

with_grade as (
    select
        *,
        -- Grade reflects market positioning (not affordability rank)
        -- High score = cheap $/sqft + large homes = AFFORDABLE
        -- Low score  = expensive $/sqft + small homes = PREMIUM market
        case
            when insufficient_data = true  then 'INSUFFICIENT DATA'
            when housing_score >= 65       then 'AFFORDABLE'
            when housing_score >= 35       then 'AVERAGE'
            else                                'PREMIUM'
        end                                                                 as housing_grade
    from with_final_score
),

-- ── Cortex narrative ──────────────────────────────────────────────────────
cortex_desc as (
    select
        location_id,
        upper(neighborhood_name)                                            as neighborhood_name,
        upper(city)                                                         as city,
        sqmiles,
        upper(distribution_group)                                           as distribution_group,
        total_properties,
        boston_count,
        cambridge_count,
        greater_boston_count,
        condo_count,
        rental_count,
        residential_count,
        avg_living_area_sqft,
        avg_bedrooms,
        avg_full_baths,
        avg_assessed_value,
        avg_estimated_rent,
        avg_price_per_sqft,
        min_assessed_value,
        max_assessed_value,
        excellent_count,
        very_good_count,
        good_count,
        average_count,
        fair_count,
        avg_condition_score,
        has_ac_count,
        has_parking_count,
        has_fireplace_count,
        avg_property_age,
        oldest_year_built,
        newest_year_built,
        properties_per_sqmile,
        pct_good_condition,
        pct_has_ac,
        pct_has_parking,
        amenity_rate,
        insufficient_data,
        pass1_score,
        housing_score,
        housing_grade,

        iff(
            insufficient_data = true,
            'Insufficient housing data for ' || upper(neighborhood_name)
                || '. This area has fewer than 50 residential properties in the dataset.',
            snowflake.cortex.complete(
                'mistral-large',
                'You are a housing analyst writing for a Boston/Cambridge area neighborhood guide. '
                || 'Write a concise 2-3 sentence housing market summary for '
                || upper(neighborhood_name) || ', located in ' || upper(city) || ', Massachusetts. '
                || 'IMPORTANT: ' || upper(neighborhood_name) || ' is in ' || upper(city) || ', Massachusetts. '
                || 'Do not confuse it with any place of the same name elsewhere. '
                || 'Do not use bullet points. Write in plain prose only. '
                || 'Focus on affordability (price per sq ft), typical home size, and housing stock. '
                || case
                    when distribution_group = 'GREATER_BOSTON'
                    then 'NOTE: Data comes from MassGIS L3 parcel records. '
                    else ''
                   end
                || 'Housing score: ' || round(housing_score, 1) || '/100 (' || housing_grade || '). '
                || 'Total residential properties: ' || total_properties || '. '
                || 'Average assessed value: $' || to_char(coalesce(avg_assessed_value, 0), '999,999,999') || '. '
                || 'Average price per sqft: $' || coalesce(avg_price_per_sqft::varchar, 'N/A') || '. '
                || 'Average living area: ' || coalesce(avg_living_area_sqft::varchar, 'N/A') || ' sq ft. '
                || 'Average estimated monthly rent: $' || to_char(coalesce(avg_estimated_rent, 0), '999,999') || '. '
                || 'Condos: ' || condo_count || ', rentals: ' || rental_count || '. '
                || 'Average property age: ' || coalesce(avg_property_age::varchar, 'N/A') || ' years.'
            )
        )                                                                   as row_description,

        to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS')                as load_timestamp

    from with_grade
)

select
    location_id,
    neighborhood_name,
    city,
    sqmiles,
    distribution_group,
    total_properties,
    boston_count,
    cambridge_count,
    greater_boston_count,
    condo_count,
    rental_count,
    residential_count,
    avg_living_area_sqft,
    avg_bedrooms,
    avg_full_baths,
    avg_assessed_value,
    avg_estimated_rent,
    avg_price_per_sqft,
    min_assessed_value,
    max_assessed_value,
    excellent_count,
    very_good_count,
    good_count,
    average_count,
    fair_count,
    avg_condition_score,
    has_ac_count,
    has_parking_count,
    has_fireplace_count,
    avg_property_age,
    oldest_year_built,
    newest_year_built,
    properties_per_sqmile,
    pct_good_condition,
    pct_has_ac,
    pct_has_parking,
    amenity_rate,
    insufficient_data,
    pass1_score,
    housing_score,
    housing_grade,
    row_description,
    load_timestamp
from cortex_desc
order by housing_score desc nulls last