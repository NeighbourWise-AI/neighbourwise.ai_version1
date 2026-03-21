{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH int_schools AS (
    SELECT * FROM {{ ref('INT_BOSTON_SCHOOLS') }}
),
master_location AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        geometry
    FROM {{ ref('MASTER_LOCATION') }}
    WHERE has_geometry = TRUE
    -- NO IS_BOSTON / IS_CAMBRIDGE filter here
    -- All 51 rows (26 Boston + 13 Cambridge + 12 Greater Boston) must be eligible
),
with_neighborhood AS (
    SELECT
        s.*,
        ml.location_id       AS location_id,
        ml.neighborhood_name AS neighborhood_name,
        ml.city              AS city,
        'SPATIAL'            AS resolution_method
    FROM int_schools s
    INNER JOIN master_location ml       -- INNER JOIN: drops schools with no polygon match
        ON s.has_valid_location = TRUE
        AND ST_CONTAINS(
            ml.geometry,
            ST_MAKEPOINT(s.long, s.lat)
        )
),

with_description AS (
    SELECT
        school_id,
        school_name,
        address,
        town_mail,
        town,
        zipcode,
        phone,
        school_type_code,
        school_type_desc,
        is_public,
        is_private,
        is_charter,
        is_special_education,
        is_vocational,
        grades_raw,
        grade_low,
        grade_high,
        school_level,
        serves_elementary,
        serves_middle,
        serves_high,
        ma_addr_id,
        lat,
        long,
        has_valid_location,
        location_id,
        neighborhood_name,
        city,
        resolution_method,
        school_name || ' is a ' ||
        LOWER(school_type_desc) || ' school located in ' ||
        neighborhood_name || ', ' || city || '.' ||
        ' It serves grades ' || grade_low || ' through ' || grade_high ||
        ' and is classified as a ' || LOWER(school_level) || ' school.' ||
        CASE WHEN is_public            THEN ' It is a public school.'                        ELSE '' END ||
        CASE WHEN is_private           THEN ' It is a private school.'                       ELSE '' END ||
        CASE WHEN is_charter           THEN ' It is a charter school.'                       ELSE '' END ||
        CASE WHEN is_vocational        THEN ' It offers vocational programs.'                ELSE '' END ||
        CASE WHEN is_special_education THEN ' It offers special education programs.'         ELSE '' END ||
        CASE WHEN serves_elementary AND serves_high
                                       THEN ' It is a K-12 school serving all grade levels.' ELSE '' END
        AS row_description,
        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp

    FROM with_neighborhood
)

SELECT *
FROM with_description
WHERE school_id IS NOT NULL