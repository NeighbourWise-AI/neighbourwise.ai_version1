-- INT_BOSTON_RESTAURANTS
-- Cleans and enriches STG_BOSTON_RESTAURANTS.
-- No geocoding needed — Yelp provides lat/long directly.
-- Spatial join to MASTER_LOCATION happens at MRT level.
-- Source: STG_BOSTON_RESTAURANTS

{{ 
    config
    (
        materialized='table'
    ) 
}}

WITH base AS (
    SELECT
        COALESCE(NULLIF(TRIM(ID), ''), 'UNKNOWN')                       AS restaurant_id,
        UPPER(COALESCE(NULLIF(TRIM(NAME), ''), 'UNKNOWN'))               AS restaurant_name,
        CASE
            WHEN UPPER(TRIM(IS_CLOSED)) = 'TRUE'  THEN TRUE
            WHEN UPPER(TRIM(IS_CLOSED)) = 'FALSE' THEN FALSE
            ELSE FALSE
        END                                                              AS is_closed,
        COALESCE(CAST(REVIEW_COUNT AS INTEGER), 0)                       AS review_count,
        COALESCE(ROUND(CAST(RATING AS FLOAT), 1), 0.0)                  AS rating,
        UPPER(COALESCE(NULLIF(TRIM(PRICE), ''), 'UNKNOWN'))              AS price,
        UPPER(COALESCE(NULLIF(TRIM(DISPLAY_PHONE), ''), 'UNKNOWN'))      AS display_phone,
        UPPER(COALESCE(NULLIF(TRIM(CATEGORIES_TITLES), ''), 'UNKNOWN'))  AS categories_titles,
        UPPER(COALESCE(NULLIF(TRIM(CATEGORIES_ALIASES), ''), 'UNKNOWN')) AS categories_aliases,
        UPPER(COALESCE(NULLIF(TRIM(TRANSACTIONS), ''), 'UNKNOWN'))       AS transactions,
        COALESCE(LATITUDE, -999.0)                                       AS latitude,
        COALESCE(LONGITUDE, -999.0)                                      AS longitude,
        UPPER(COALESCE(NULLIF(TRIM(ADDRESS1), ''), 'UNKNOWN'))           AS address1,
        UPPER(COALESCE(NULLIF(TRIM(CITY), ''), 'UNKNOWN'))               AS city,
        UPPER(COALESCE(NULLIF(TRIM(STATE), ''), 'UNKNOWN'))              AS state,
        CASE
            WHEN NULLIF(TRIM(ZIP_CODE), '') IS NULL THEN 'UNKNOWN'
            ELSE LPAD(TRIM(ZIP_CODE), 5, '0')
        END AS ZIP_CODE 
        -- Dropped: PHONE (use DISPLAY_PHONE), ADDRESS2/3 (mostly empty/institutional),
        --          DISPLAY_ADDRESS (reconstructible), ALIAS (Yelp slug),
        --          IMAGE_URL, URL (not needed in data layer),
        --          COUNTRY (always US), DISTANCE_M (API artifact),
        --          YELP_LOCATION (redundant with CITY/STATE)
    FROM {{ source('stage', 'stg_boston_restaurants') }}
    WHERE IS_CLOSED = 'FALSE'
      AND LATITUDE  IS NOT NULL
      AND LONGITUDE IS NOT NULL
      AND NAME      IS NOT NULL
),

filtered AS (
    SELECT *
    FROM base
    -- Bounding box: Greater Boston
    WHERE latitude  BETWEEN 42.20 AND 42.55
      AND longitude BETWEEN -71.35 AND -70.85
),

enriched AS (
    SELECT
        restaurant_id,
        restaurant_name,
        display_phone,
        categories_titles,
        categories_aliases,
        transactions,
        latitude,
        longitude,
        address1,
        city,
        state,
        zip_code,

        -- ── Location validity ──────────────────────────────────────────────
        TRUE                                                             AS has_valid_location,

        -- ── Price tier ─────────────────────────────────────────────────────
        CASE price
            WHEN '$'    THEN 1
            WHEN '$$'   THEN 2
            WHEN '$$$'  THEN 3
            WHEN '$$$$' THEN 4
            ELSE 0
        END                                                              AS price_tier,
        price                                                            AS price_label,

        -- ── Rating ─────────────────────────────────────────────────────────
        rating,
        review_count,

        CASE
            WHEN rating >= 4.5 THEN 'EXCELLENT'
            WHEN rating >= 4.0 THEN 'GOOD'
            WHEN rating >= 3.0 THEN 'AVERAGE'
            WHEN rating >  0.0 THEN 'POOR'
            ELSE 'UNRATED'
        END                                                              AS rating_tier,

        -- ── Review volume tier ─────────────────────────────────────────────
        CASE
            WHEN review_count >= 500 THEN 'VERY_HIGH'
            WHEN review_count >= 200 THEN 'HIGH'
            WHEN review_count >= 50  THEN 'MODERATE'
            WHEN review_count >  0   THEN 'LOW'
            ELSE 'UNKNOWN'
        END                                                              AS review_volume_tier,

        -- ── Delivery / pickup flags ────────────────────────────────────────
        CASE
            WHEN transactions LIKE '%DELIVERY%' THEN TRUE
            ELSE FALSE
        END                                                              AS has_delivery,

        CASE
            WHEN transactions LIKE '%PICKUP%' THEN TRUE
            ELSE FALSE
        END                                                              AS has_pickup,

        -- ── Primary cuisine category ───────────────────────────────────────
        CASE
            WHEN categories_aliases RLIKE
                'COFFEE|CAFES|TEA|JUICEBARS|BAKERIES|BAGELS|DONUTS'
                THEN 'CAFE_BAKERY'
            WHEN categories_aliases RLIKE
                'BEERBAR|DIVEBARS|SPORTSBARS|PUBS|COCKTAILBARS'
                THEN 'BAR'
            WHEN categories_aliases RLIKE
                'HOTDOGS|BURGERS|FASTFOOD|CHICKEN_WINGS'
                THEN 'FAST_FOOD'
            WHEN categories_aliases RLIKE
                'BREAKFAST_BRUNCH'
                THEN 'BREAKFAST'
            WHEN categories_aliases RLIKE
                'PIZZA'
                THEN 'PIZZA'
            WHEN categories_aliases RLIKE
                'SANDWICHES|DELIS'
                THEN 'SANDWICHES_DELI'
            WHEN categories_aliases RLIKE
                'SALAD|VEGETARIAN|VEGAN|GLUTEN_FREE'
                THEN 'HEALTHY'
            WHEN categories_aliases RLIKE
                'MEXICAN|ITALIAN|CHINESE|INDPAK|KOREAN|MIDEASTERN|JAPANESE|THAI|VIETNAMESE|GREEK|FRENCH|CARIBBEAN|LATIN|BRAZILIAN|HIMALAYAN|PUERTORICAN|DOMINICAN|ASIANFUSION'
                THEN 'ETHNIC'
            WHEN categories_aliases RLIKE
                'TRADAMERICAN|NEWAMERICAN'
                THEN 'AMERICAN'
            WHEN categories_aliases = 'UNKNOWN'
                THEN 'UNKNOWN'
            ELSE 'OTHER'
        END                                                              AS cuisine_category,

        CURRENT_TIMESTAMP()                                              AS load_timestamp

    FROM filtered
)

SELECT * FROM enriched