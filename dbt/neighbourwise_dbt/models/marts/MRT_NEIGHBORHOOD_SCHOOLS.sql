{{ 
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH school_agg AS (
    SELECT
        ml.location_id,
        ml.neighborhood_name,
        ml.city,
        ml.sqmiles,

        COUNT(s.school_id)                                              AS total_schools,
        SUM(CASE WHEN s.is_public            = TRUE THEN 1 ELSE 0 END) AS public_school_count,
        SUM(CASE WHEN s.is_private           = TRUE THEN 1 ELSE 0 END) AS private_school_count,
        SUM(CASE WHEN s.is_charter           = TRUE THEN 1 ELSE 0 END) AS charter_school_count,
        SUM(CASE WHEN s.is_special_education = TRUE THEN 1 ELSE 0 END) AS special_ed_count,
        SUM(CASE WHEN s.is_vocational        = TRUE THEN 1 ELSE 0 END) AS vocational_count,

        SUM(CASE WHEN s.serves_elementary = TRUE THEN 1 ELSE 0 END)    AS elementary_count,
        SUM(CASE WHEN s.serves_middle     = TRUE THEN 1 ELSE 0 END)    AS middle_count,
        SUM(CASE WHEN s.serves_high       = TRUE THEN 1 ELSE 0 END)    AS high_count,
        SUM(CASE WHEN s.serves_elementary = TRUE
                  AND s.serves_middle     = TRUE
                  AND s.serves_high       = TRUE THEN 1 ELSE 0 END)    AS k12_count,

        MAX(CASE WHEN s.serves_elementary = TRUE THEN 1 ELSE 0 END)    AS has_elementary,
        MAX(CASE WHEN s.serves_middle     = TRUE THEN 1 ELSE 0 END)    AS has_middle,
        MAX(CASE WHEN s.serves_high       = TRUE THEN 1 ELSE 0 END)    AS has_high,
        MAX(CASE WHEN s.is_public         = TRUE THEN 1 ELSE 0 END)    AS has_public,
        MAX(CASE WHEN s.is_charter        = TRUE THEN 1 ELSE 0 END)    AS has_charter

    FROM {{ ref('MASTER_LOCATION') }} ml
    LEFT JOIN {{ ref('MRT_BOSTON_SCHOOLS') }} s
        ON s.location_id = ml.location_id
    GROUP BY
        ml.location_id,
        ml.neighborhood_name,
        ml.city,
        ml.sqmiles
),

scored AS (
    SELECT
        *,

        CASE WHEN sqmiles > 0
             THEN ROUND(total_schools / sqmiles, 2)
             ELSE 0
        END AS schools_per_sqmile,

        CASE WHEN total_schools > 0
             THEN ROUND(public_school_count  * 100.0 / total_schools, 1) ELSE 0
        END AS pct_public,
        CASE WHEN total_schools > 0
             THEN ROUND(private_school_count * 100.0 / total_schools, 1) ELSE 0
        END AS pct_private,
        CASE WHEN total_schools > 0
             THEN ROUND(charter_school_count * 100.0 / total_schools, 1) ELSE 0
        END AS pct_charter,

        (CASE WHEN has_public          = 1 THEN 1 ELSE 0 END +
         CASE WHEN has_charter         = 1 THEN 1 ELSE 0 END +
         CASE WHEN private_school_count > 0 THEN 1 ELSE 0 END) AS school_type_diversity,

        (has_elementary + has_middle + has_high)                AS level_coverage_score,

        LEAST(
            CASE WHEN sqmiles > 0
                 THEN ROUND((total_schools / sqmiles) / 5.0 * 40, 1)
                 ELSE 0
            END, 40
        ) AS density_score,

        ROUND((has_elementary + has_middle + has_high) / 3.0 * 30, 1) AS coverage_score,

        LEAST(ROUND(total_schools / 20.0 * 20, 1), 20) AS count_score,

        ROUND(
            (CASE WHEN has_public          = 1 THEN 1 ELSE 0 END +
             CASE WHEN has_charter         = 1 THEN 1 ELSE 0 END +
             CASE WHEN private_school_count > 0 THEN 1 ELSE 0 END)
            / 3.0 * 10, 1
        ) AS diversity_score

    FROM school_agg
),

final_scored AS (
    SELECT
        *,

        LEAST(
            ROUND(density_score + coverage_score + count_score + diversity_score, 1),
            100
        ) AS school_score,

        CASE
            WHEN LEAST(ROUND(density_score + coverage_score + count_score + diversity_score, 1), 100) >= 75
                THEN 'EXCELLENT'
            WHEN LEAST(ROUND(density_score + coverage_score + count_score + diversity_score, 1), 100) >= 50
                THEN 'GOOD'
            WHEN LEAST(ROUND(density_score + coverage_score + count_score + diversity_score, 1), 100) >= 25
                THEN 'MODERATE'
            ELSE 'LIMITED'
        END AS school_grade,

        neighborhood_name || ' (' || city || ') has ' ||
        total_schools || ' schools (' ||
        public_school_count  || ' public, ' ||
        charter_school_count || ' charter, ' ||
        private_school_count || ' private). ' ||
        'Level coverage: ' ||
        CASE WHEN has_elementary = 1 THEN 'Elementary ' ELSE '' END ||
        CASE WHEN has_middle     = 1 THEN 'Middle '     ELSE '' END ||
        CASE WHEN has_high       = 1 THEN 'High School ' ELSE '' END ||
        '| Schools per sq mile: ' ||
        CASE WHEN sqmiles > 0
             THEN ROUND(total_schools / sqmiles, 2)::VARCHAR
             ELSE '0'
        END AS row_description

    FROM scored
),

cortex_desc AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        sqmiles,
        total_schools,
        public_school_count,
        private_school_count,
        charter_school_count,
        special_ed_count,
        vocational_count,
        elementary_count,
        middle_count,
        high_count,
        k12_count,
        has_elementary,
        has_middle,
        has_high,
        has_public,
        has_charter,
        schools_per_sqmile,
        pct_public,
        pct_private,
        pct_charter,
        school_type_diversity,
        level_coverage_score,
        density_score,
        coverage_score,
        count_score,
        diversity_score,
        school_score,
        school_grade,

        -- FIX: mistral-7b → mistral-large
        -- FIX: Added Massachusetts grounding + IMPORTANT instruction
        -- FIX: Added Boston/Cambridge area framing to prevent city hallucinations
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'You are a neighborhood analyst writing for a Boston/Cambridge, Massachusetts neighborhood guide. '
            || 'In 2-3 sentences, describe the school landscape for '
            || neighborhood_name || ', located in ' || city || ', Massachusetts. '
            || 'IMPORTANT: ' || neighborhood_name
            || ' is a neighborhood or city in the Boston/Cambridge, Massachusetts area. '
            || 'Do not confuse it with any place of the same name elsewhere. '
            || 'It has ' || total_schools || ' total schools ('
            || public_school_count  || ' public, '
            || charter_school_count || ' charter, '
            || private_school_count || ' private). '
            || 'Level coverage: elementary=' || has_elementary::VARCHAR
            || ', middle='                   || has_middle::VARCHAR
            || ', high='                     || has_high::VARCHAR || '. '
            || 'School score: '              || school_score::VARCHAR
            || '/100 ('                      || school_grade || '). '
            || 'Schools per square mile: '   || schools_per_sqmile::VARCHAR || '. '
            || 'Be factual and concise. Do not invent data.'
        ) AS row_description

    FROM final_scored
)

SELECT
    location_id,
    neighborhood_name,
    city,
    sqmiles,
    total_schools,
    public_school_count,
    private_school_count,
    charter_school_count,
    special_ed_count,
    vocational_count,
    elementary_count,
    middle_count,
    high_count,
    k12_count,
    has_elementary,
    has_middle,
    has_high,
    has_public,
    has_charter,
    schools_per_sqmile,
    pct_public,
    pct_private,
    pct_charter,
    school_type_diversity,
    level_coverage_score,
    school_score,
    school_grade,
    row_description,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM cortex_desc