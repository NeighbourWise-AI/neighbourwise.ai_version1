{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH restaurant_agg AS (
    SELECT
        ml.location_id,
        ml.neighborhood_name,
        ml.city,
        ml.sqmiles,

        -- Total counts
        COUNT(r.restaurant_id)                                              AS total_restaurants,

        -- By price tier
        SUM(CASE WHEN r.price_tier = 1 THEN 1 ELSE 0 END)                  AS budget_count,
        SUM(CASE WHEN r.price_tier = 2 THEN 1 ELSE 0 END)                  AS mid_range_count,
        SUM(CASE WHEN r.price_tier >= 3 THEN 1 ELSE 0 END)                 AS upscale_count,
        SUM(CASE WHEN r.price_tier = 0 THEN 1 ELSE 0 END)                  AS no_price_count,

        -- By rating tier
        SUM(CASE WHEN r.rating_tier = 'EXCELLENT' THEN 1 ELSE 0 END)       AS excellent_count,
        SUM(CASE WHEN r.rating_tier = 'GOOD'      THEN 1 ELSE 0 END)       AS good_count,
        SUM(CASE WHEN r.rating_tier = 'AVERAGE'   THEN 1 ELSE 0 END)       AS average_count,
        SUM(CASE WHEN r.rating_tier = 'POOR'      THEN 1 ELSE 0 END)       AS poor_count,

        -- Quality metrics
        ROUND(AVG(CASE WHEN r.rating > 0 THEN r.rating END), 2)            AS avg_rating,
        SUM(r.review_count)                                                 AS total_reviews,

        -- Delivery / pickup access
        SUM(CASE WHEN r.has_delivery THEN 1 ELSE 0 END)                    AS delivery_count,
        SUM(CASE WHEN r.has_pickup   THEN 1 ELSE 0 END)                    AS pickup_count,

        -- Cuisine diversity — count of distinct cuisine categories
        COUNT(DISTINCT r.cuisine_category)                                  AS cuisine_diversity,

        -- Cuisine category breakdown
        SUM(CASE WHEN r.cuisine_category = 'ETHNIC'         THEN 1 ELSE 0 END) AS ethnic_count,
        SUM(CASE WHEN r.cuisine_category = 'PIZZA'          THEN 1 ELSE 0 END) AS pizza_count,
        SUM(CASE WHEN r.cuisine_category = 'CAFE_BAKERY'    THEN 1 ELSE 0 END) AS cafe_bakery_count,
        SUM(CASE WHEN r.cuisine_category = 'FAST_FOOD'      THEN 1 ELSE 0 END) AS fast_food_count,
        SUM(CASE WHEN r.cuisine_category = 'BREAKFAST'      THEN 1 ELSE 0 END) AS breakfast_count,
        SUM(CASE WHEN r.cuisine_category = 'SANDWICHES_DELI'THEN 1 ELSE 0 END) AS sandwiches_deli_count,
        SUM(CASE WHEN r.cuisine_category = 'HEALTHY'        THEN 1 ELSE 0 END) AS healthy_count,
        SUM(CASE WHEN r.cuisine_category = 'AMERICAN'       THEN 1 ELSE 0 END) AS american_count,
        SUM(CASE WHEN r.cuisine_category = 'BAR'            THEN 1 ELSE 0 END) AS bar_count,
        SUM(CASE WHEN r.cuisine_category = 'OTHER'          THEN 1 ELSE 0 END) AS other_count

    FROM {{ ref('MASTER_LOCATION') }} ml
    LEFT JOIN {{ ref('MRT_BOSTON_RESTAURANTS') }} r
        ON r.location_id = ml.location_id
    GROUP BY
        ml.location_id,
        ml.neighborhood_name,
        ml.city,
        ml.sqmiles
),

scored AS (
    SELECT
        *,

        -- Density: restaurants per square mile
        CASE
            WHEN sqmiles > 0 THEN ROUND(total_restaurants / sqmiles, 2)
            ELSE 0
        END                                                                 AS restaurants_per_sqmile,

        -- Pct high quality (excellent + good)
        CASE
            WHEN total_restaurants > 0
            THEN ROUND((excellent_count + good_count) * 100.0 / total_restaurants, 1)
            ELSE 0
        END                                                                 AS pct_high_quality,

        -- Pct with delivery
        CASE
            WHEN total_restaurants > 0
            THEN ROUND(delivery_count * 100.0 / total_restaurants, 1)
            ELSE 0
        END                                                                 AS pct_delivery,

        -- Score components (0-100 total):
        -- 40 pts: density (restaurants per sqmile, capped at 20 for full score)
        -- 30 pts: quality (pct high quality, 100% = full score)
        -- 20 pts: total count (capped at 100 restaurants for full score)
        -- 10 pts: cuisine diversity (10 distinct categories = full score)
        LEAST(
            CASE WHEN sqmiles > 0
                 THEN ROUND((total_restaurants / sqmiles) / 20.0 * 40, 1)
                 ELSE 0
            END,
            40
        )                                                                   AS density_score,

        CASE
            WHEN total_restaurants > 0
            THEN ROUND((excellent_count + good_count) * 1.0 / total_restaurants * 30, 1)
            ELSE 0
        END                                                                 AS quality_score,

        LEAST(ROUND(total_restaurants / 100.0 * 20, 1), 20)               AS count_score,

        LEAST(ROUND(cuisine_diversity / 10.0 * 10, 1), 10)                AS diversity_score

    FROM restaurant_agg
),

final_scored AS (
    SELECT
        *,

        LEAST(
            ROUND(density_score + quality_score + count_score + diversity_score, 1),
            100
        ) AS restaurant_score,

        CASE
            WHEN LEAST(ROUND(density_score + quality_score + count_score + diversity_score, 1), 100) >= 75
                THEN 'EXCELLENT'
            WHEN LEAST(ROUND(density_score + quality_score + count_score + diversity_score, 1), 100) >= 50
                THEN 'GOOD'
            WHEN LEAST(ROUND(density_score + quality_score + count_score + diversity_score, 1), 100) >= 25
                THEN 'MODERATE'
            ELSE 'LIMITED'
        END AS restaurant_grade,

        -- Row description
        neighborhood_name || ' (' || city || ') HAS ' ||
        total_restaurants || ' RESTAURANTS. ' ||
        'AVG RATING: ' || COALESCE(avg_rating::VARCHAR, 'N/A') || '/5. ' ||
        'CUISINE DIVERSITY: ' || cuisine_diversity || ' CATEGORIES. ' ||
        'DENSITY: ' ||
        CASE WHEN sqmiles > 0
             THEN ROUND(total_restaurants / sqmiles, 2)::VARCHAR
             ELSE '0'
        END || ' RESTAURANTS/SQMILE. ' ||
        'WITH DELIVERY: ' || delivery_count || ' RESTAURANTS.' AS row_description

    FROM scored
),

-- Cortex description — list columns explicitly (never use c.*)
cortex_desc AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        sqmiles,
        total_restaurants,
        budget_count,
        mid_range_count,
        upscale_count,
        no_price_count,
        excellent_count,
        good_count,
        average_count,
        poor_count,
        avg_rating,
        total_reviews,
        delivery_count,
        pickup_count,
        cuisine_diversity,
        ethnic_count,
        pizza_count,
        cafe_bakery_count,
        fast_food_count,
        breakfast_count,
        sandwiches_deli_count,
        healthy_count,
        american_count,
        bar_count,
        other_count,
        restaurants_per_sqmile,
        pct_high_quality,
        pct_delivery,
        density_score,
        quality_score,
        count_score,
        diversity_score,
        restaurant_score,
        restaurant_grade,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'You are a neighborhood analyst writing for a Boston/Cambridge area neighborhood guide. ' ||
            'In 2-3 sentences, describe the restaurant scene for ' ||
            neighborhood_name || ', located in ' || city || ', Massachusetts. ' ||
            'It has ' || total_restaurants || ' restaurants with an average rating of ' ||
            COALESCE(avg_rating::VARCHAR, 'unknown') || '/5. ' ||
            'Cuisine diversity: ' || cuisine_diversity || ' categories including ' ||
            ethnic_count || ' ethnic, ' || pizza_count || ' pizza, ' ||
            cafe_bakery_count || ' cafe/bakery. ' ||
            'Price range: ' || budget_count || ' budget ($), ' ||
            mid_range_count || ' mid-range ($$), ' ||
            upscale_count || ' upscale ($$$+). ' ||
            'Restaurant score: ' || restaurant_score::VARCHAR ||
            '/100 (' || restaurant_grade || '). ' ||
            'IMPORTANT: ' || neighborhood_name || ' is a neighborhood or city in Massachusetts, USA. ' ||
            'Do not confuse it with any other city or state. ' ||
            'Be factual and concise. Do not invent data.'
        ) AS row_description
    FROM final_scored
)

SELECT
    location_id,
    neighborhood_name,
    city,
    sqmiles,
    total_restaurants,
    budget_count,
    mid_range_count,
    upscale_count,
    no_price_count,
    excellent_count,
    good_count,
    average_count,
    poor_count,
    avg_rating,
    total_reviews,
    delivery_count,
    pickup_count,
    cuisine_diversity,
    ethnic_count,
    pizza_count,
    cafe_bakery_count,
    fast_food_count,
    breakfast_count,
    sandwiches_deli_count,
    healthy_count,
    american_count,
    bar_count,
    other_count,
    restaurants_per_sqmile,
    pct_high_quality,
    pct_delivery,
    restaurant_score,
    restaurant_grade,
    row_description,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM cortex_desc