{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

with grocery_agg as (
    select
        ml.location_id,
        ml.name                                                             as neighborhood_name,
        ml.city,
        ml.sqmiles,

        -- ── Total counts ──────────────────────────────────────────────────────
        count(g.objectid)                                                   as total_stores,

        -- ── Store type breakdown (from STORE_TYPE) ────────────────────────────
        sum(case when g.store_type = 'SUPERMARKET'        then 1 else 0 end) as supermarket_count,
        sum(case when g.store_type = 'CONVENIENCE_STORE'  then 1 else 0 end) as convenience_store_count,
        sum(case when g.store_type = 'PHARMACY'           then 1 else 0 end) as pharmacy_count,
        sum(case when g.store_type = 'FRUIT_VEG_MARKET'   then 1 else 0 end) as fruit_veg_count,
        sum(case when g.store_type = 'MEAT_MARKET'        then 1 else 0 end) as meat_market_count,
        sum(case when g.store_type = 'FISH_SEAFOOD_MARKET'then 1 else 0 end) as fish_seafood_count,
        sum(case when g.store_type = 'SPECIALTY_FOOD_STORE'then 1 else 0 end) as specialty_food_count,
        sum(case when g.store_type = 'DOLLAR_STORE'       then 1 else 0 end) as dollar_store_count,
        sum(case when g.store_type = 'WAREHOUSE_CLUB'     then 1 else 0 end) as warehouse_club_count,
        sum(case when g.store_type = 'DEPARTMENT_STORE'   then 1 else 0 end) as department_store_count,
        sum(case when g.store_type in ('FARMERS_MARKET','WINTER_MARKET')
                 then 1 else 0 end)                                         as farmers_market_count,

        -- ── Flag-based counts (from boolean columns) ──────────────────────────
        sum(case when g.is_essential_food_source  then 1 else 0 end)       as essential_store_count,
        sum(case when g.is_pharmacy_or_drug_store then 1 else 0 end)       as pharmacy_drug_count,
        sum(case when g.is_specialty_store        then 1 else 0 end)       as specialty_store_count,
        sum(case when g.is_large_format           then 1 else 0 end)       as large_format_count,
        sum(case when g.is_convenience_type       then 1 else 0 end)       as convenience_type_count,

        -- ── Survey year breakdown ─────────────────────────────────────────────
        sum(case when g.data_year = 2021 then 1 else 0 end)                as stores_2021,
        sum(case when g.data_year = 2017 then 1 else 0 end)                as stores_2017,
        max(g.data_year)                                                    as latest_data_year

    from {{ source('STAGE', 'STG_MASTER_LOCATION') }} ml
    left join (
        select *
        from {{ ref('MRT_BOSTON_GROCERY_STORES') }}
        qualify row_number() over (
            partition by upper(trim(store_name)), upper(trim(street_address)), location_id
            order by data_year desc
        ) = 1
    ) g
        on g.location_id = ml.location_id
    group by
        ml.location_id,
        ml.name,
        ml.city,
        ml.sqmiles
),

scored as (
    select
        *,

        -- Stores per square mile
        case
            when sqmiles > 0 then round(total_stores / sqmiles, 2)
            else 0
        end                                                                 as stores_per_sqmile,

        -- Essential store percentage
        case
            when total_stores > 0
            then round(essential_store_count * 100.0 / total_stores, 1)
            else 0
        end                                                                 as pct_essential,

        -- Convenience type percentage
        case
            when total_stores > 0
            then round(convenience_type_count * 100.0 / total_stores, 1)
            else 0
        end                                                                 as pct_convenience,

        -- Score components (0–100 total):
        -- 40 pts: essential store density (essential per sqmile, capped at 10)
        -- 30 pts: essential store share (100% = full score)
        -- 20 pts: total store count (capped at 100 stores for full score)
        -- 10 pts: store type diversity (11 distinct types = full score)
        least(
            case
                when sqmiles > 0
                then round((essential_store_count / sqmiles) / 10.0 * 40, 1)
                else 0
            end,
            40
        )                                                                   as density_score,

        case
            when total_stores > 0
            then round(essential_store_count * 1.0 / total_stores * 30, 1)
            else 0
        end                                                                 as essential_score,

        least(round(total_stores / 100.0 * 20, 1), 20)                    as count_score,

        least(
            round(
                (
                    case when supermarket_count     > 0 then 1 else 0 end +
                    case when convenience_store_count > 0 then 1 else 0 end +
                    case when pharmacy_count         > 0 then 1 else 0 end +
                    case when fruit_veg_count        > 0 then 1 else 0 end +
                    case when meat_market_count      > 0 then 1 else 0 end +
                    case when fish_seafood_count     > 0 then 1 else 0 end +
                    case when specialty_food_count   > 0 then 1 else 0 end +
                    case when dollar_store_count     > 0 then 1 else 0 end +
                    case when warehouse_club_count   > 0 then 1 else 0 end +
                    case when department_store_count > 0 then 1 else 0 end +
                    case when farmers_market_count   > 0 then 1 else 0 end
                ) / 11.0 * 10,
                1
            ),
            10
        )                                                                   as diversity_score

    from grocery_agg
),

final_scored as (
    select
        *,

        least(
            round(density_score + essential_score + count_score + diversity_score, 1),
            100
        )                                                                   as grocery_score,

        case
            when least(round(density_score + essential_score + count_score + diversity_score, 1), 100) >= 75
                then 'ABUNDANT'
            when least(round(density_score + essential_score + count_score + diversity_score, 1), 100) >= 50
                then 'WELL_STOCKED'
            when least(round(density_score + essential_score + count_score + diversity_score, 1), 100) >= 25
                then 'ADEQUATE'
            when least(round(density_score + essential_score + count_score + diversity_score, 1), 100) > 0
                then 'SCARCE'
            else 'DESERT'
        end                                                                 as grocery_grade

    from scored
),

cortex_desc as (
    select
        location_id,
        neighborhood_name,
        city,
        sqmiles,
        total_stores,
        supermarket_count,
        convenience_store_count,
        pharmacy_count,
        fruit_veg_count,
        meat_market_count,
        fish_seafood_count,
        specialty_food_count,
        dollar_store_count,
        warehouse_club_count,
        department_store_count,
        farmers_market_count,
        essential_store_count,
        pharmacy_drug_count,
        specialty_store_count,
        large_format_count,
        convenience_type_count,
        stores_2021,
        stores_2017,
        latest_data_year,
        stores_per_sqmile,
        pct_essential,
        pct_convenience,
        density_score,
        essential_score,
        count_score,
        diversity_score,
        grocery_score,
        grocery_grade,

        snowflake.cortex.complete(
            'mistral-large',
            'You are a neighborhood food access analyst writing for a Boston/Cambridge area neighborhood guide. ' ||
            'In 2-3 sentences, describe the grocery and food retail scene for ' ||
            neighborhood_name || ', located in ' || city || ', Massachusetts. ' ||
            'It has ' || total_stores || ' grocery and food retail stores. ' ||
            'Essential food sources (supermarkets, produce, meat, fish): ' || essential_store_count || '. ' ||
            'Supermarkets: ' || supermarket_count || '. ' ||
            'Convenience stores: ' || convenience_store_count || '. ' ||
            'Specialty food stores: ' || specialty_food_count || '. ' ||
            'Farmers markets: ' || farmers_market_count || '. ' ||
            'Stores per square mile: ' || stores_per_sqmile::varchar || '. ' ||
            'Essential store share: ' || pct_essential::varchar || '%. ' ||
            'Grocery access score: ' || grocery_score::varchar || '/100 (' || grocery_grade || '). ' ||
            'IMPORTANT: ' || neighborhood_name || ' is a neighborhood or city in the Greater Boston/Cambridge, Massachusetts area. ' ||
            'Do not confuse it with any other location. ' ||
            'Be factual, concise, and suitable for a neighborhood recommendation app. Do not invent data.'
        )                                                                   as row_description

    from final_scored
)

select
    location_id,
    neighborhood_name,
    city,
    sqmiles,
    total_stores,
    supermarket_count,
    convenience_store_count,
    pharmacy_count,
    fruit_veg_count,
    meat_market_count,
    fish_seafood_count,
    specialty_food_count,
    dollar_store_count,
    warehouse_club_count,
    department_store_count,
    farmers_market_count,
    essential_store_count,
    pharmacy_drug_count,
    specialty_store_count,
    large_format_count,
    convenience_type_count,
    stores_2021,
    stores_2017,
    latest_data_year,
    stores_per_sqmile,
    pct_essential,
    pct_convenience,
    grocery_score,
    grocery_grade,
    row_description,
    current_timestamp()                                                     as load_timestamp
from cortex_desc