-- MRT_BOSTON_RESTAURANTS
-- One row per restaurant with spatial join to MASTER_LOCATION.
-- Follows the same pattern as MRT_BOSTON_SCHOOLS, MRT_BOSTON_BLUEBIKES.
-- row_description built here using neighborhood context.
-- Source: INT_BOSTON_RESTAURANTS + MASTER_LOCATION

{{ 
    config(
        materialized='table'
        , schema='marts'
    ) 
}}

WITH spatial_join AS (
    SELECT
        r.restaurant_id,
        r.restaurant_name,
        r.display_phone,
        r.categories_titles,
        r.categories_aliases,
        r.transactions,
        r.latitude,
        r.longitude,
        r.address1,
        r.city,
        r.state,
        r.zip_code,
        r.has_valid_location,
        r.price_tier,
        r.price_label,
        r.rating,
        r.review_count,
        r.rating_tier,
        r.review_volume_tier,
        r.has_delivery,
        r.has_pickup,
        r.cuisine_category,

        -- Spatial join to MASTER_LOCATION
        ml.location_id,
        ml.neighborhood_name,
        ml.city                             AS neighborhood_city,

        -- Resolution method
        CASE
            WHEN ST_CONTAINS(
                ml.geometry,
                ST_MAKEPOINT(r.longitude, r.latitude)
            ) THEN 'SPATIAL'
            ELSE 'UNKNOWN'
        END                                 AS resolution_method,

        -- Row description — built here with neighborhood context
        r.restaurant_name ||
        ' IS A ' || r.cuisine_category ||
        ' RESTAURANT IN ' ||
        COALESCE(ml.neighborhood_name, r.city) ||
        ' (' || COALESCE(ml.city, r.state) || '). ' ||
        'CUISINE: ' || r.categories_titles || '. ' ||
        'RATING: ' || r.rating::VARCHAR ||
        '/5 (' || r.review_count::VARCHAR || ' REVIEWS). ' ||
        'PRICE: ' ||
        CASE WHEN r.price_label != 'UNKNOWN' THEN r.price_label ELSE 'N/A' END || '. ' ||
        'DELIVERY: ' || CASE WHEN r.has_delivery THEN 'YES' ELSE 'NO' END ||
        ' | PICKUP: '  || CASE WHEN r.has_pickup   THEN 'YES' ELSE 'NO' END
                                            AS row_description

    FROM {{ ref('INT_BOSTON_RESTAURANTS') }} r
    LEFT JOIN {{ ref('MASTER_LOCATION') }} ml
        ON ST_CONTAINS(
            ml.geometry,
            ST_MAKEPOINT(r.longitude, r.latitude)
        )
)

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
    has_valid_location,
    price_tier,
    price_label,
    rating,
    review_count,
    rating_tier,
    review_volume_tier,
    has_delivery,
    has_pickup,
    cuisine_category,
    location_id,
    neighborhood_name,
    neighborhood_city,
    resolution_method,
    row_description,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM spatial_join