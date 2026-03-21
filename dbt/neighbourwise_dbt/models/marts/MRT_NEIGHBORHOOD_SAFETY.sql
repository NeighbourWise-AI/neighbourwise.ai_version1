{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH crime AS (
    SELECT * FROM {{ ref('MRT_BOSTON_CRIME') }}
    WHERE location_id IS NOT NULL
),

master_location AS (
    SELECT
        location_id,
        neighborhood_name,
        city,
        sqmiles,
        is_boston,
        is_cambridge,
        is_greater_boston
    FROM {{ ref('MASTER_LOCATION') }}
),

date_bounds AS (
    SELECT
        MAX(occurred_on_date)                                               AS latest_date,
        DATEADD('month', -12, MAX(occurred_on_date))                        AS twelve_months_ago,
        DATEADD('month', -24, MAX(occurred_on_date))                        AS twenty_four_months_ago
    FROM crime
),

-- ═══════════════════════════════════════════════════════════════════════════
-- TRACK 1: Incident-level cities (Boston, Cambridge, Somerville)
-- Stats derived from MRT_BOSTON_CRIME row-level data
-- ═══════════════════════════════════════════════════════════════════════════

crime_weighted AS (
    SELECT
        c.location_id,
        c.neighborhood_name,
        c.city,
        c.occurred_on_date,
        c.crime_severity_label,
        c.is_violent_crime,
        c.is_property_crime,
        c.is_drug_related,
        c.time_of_day,
        c.is_weekend,
        c.offense_description,
        CASE
            WHEN c.occurred_on_date >= (SELECT twelve_months_ago FROM date_bounds)
            THEN 2.0 ELSE 1.0
        END AS recency_weight
    FROM crime c
),

incident_stats AS (
    SELECT
        c.location_id,
        c.neighborhood_name,
        c.city,

        COUNT(*)                                                                    AS total_incidents,
        SUM(c.recency_weight)                                                       AS weighted_incidents,
        COUNT(DISTINCT DATE_TRUNC('month', c.occurred_on_date))                     AS active_months,

        SUM(CASE WHEN c.crime_severity_label = 'HIGH'   THEN 1 ELSE 0 END)         AS high_severity_count,
        SUM(CASE WHEN c.crime_severity_label = 'MEDIUM' THEN 1 ELSE 0 END)         AS medium_severity_count,
        SUM(CASE WHEN c.crime_severity_label = 'LOW'    THEN 1 ELSE 0 END)         AS low_severity_count,

        SUM(CASE WHEN c.is_violent_crime  THEN c.recency_weight ELSE 0 END)        AS weighted_violent,
        SUM(CASE WHEN c.is_property_crime THEN c.recency_weight ELSE 0 END)        AS weighted_property,
        SUM(CASE WHEN c.is_drug_related   THEN 1               ELSE 0 END)         AS drug_related_count,

        SUM(CASE WHEN c.is_violent_crime  THEN 1 ELSE 0 END)                       AS violent_crime_count,
        SUM(CASE WHEN c.is_property_crime THEN 1 ELSE 0 END)                       AS property_crime_count,

        SUM(CASE WHEN c.time_of_day IN ('NIGHT','LATE NIGHT') THEN 1 ELSE 0 END)   AS night_crime_count,
        SUM(CASE WHEN c.is_weekend THEN 1 ELSE 0 END)                              AS weekend_crime_count,

        UPPER(MODE(c.offense_description))                                          AS most_common_offense,

        SUM(CASE WHEN c.occurred_on_date >= (SELECT twelve_months_ago     FROM date_bounds)
            THEN 1 ELSE 0 END)                                                      AS incidents_last_12m,
        SUM(CASE WHEN c.occurred_on_date >= (SELECT twenty_four_months_ago FROM date_bounds)
                  AND c.occurred_on_date  < (SELECT twelve_months_ago     FROM date_bounds)
            THEN 1 ELSE 0 END)                                                      AS incidents_prior_12m,

        FALSE AS is_aggregate_source

    FROM crime_weighted c
    GROUP BY 1, 2, 3
),

-- ═══════════════════════════════════════════════════════════════════════════
-- TRACK 2: FBI aggregate cities (11 Greater Boston, IDs 41–51)
-- Stats derived from STG_GREATER_BOSTON_CRIME annual offense counts
-- ═══════════════════════════════════════════════════════════════════════════

fbi_crime AS (
    SELECT * FROM {{ source('stage', 'stg_greater_boston_crime') }}
),

fbi_city_stats AS (
    SELECT
        ml.location_id,
        ml.neighborhood_name,
        ml.city,

        SUM(f.OFFENSE_COUNT)                                                        AS total_incidents,
        SUM(f.OFFENSE_COUNT) * 1.0                                                  AS weighted_incidents,
        36                                                                          AS active_months,

        SUM(CASE WHEN f.SEVERITY = 'HIGH'   THEN f.OFFENSE_COUNT ELSE 0 END)       AS high_severity_count,
        SUM(CASE WHEN f.SEVERITY = 'MEDIUM' THEN f.OFFENSE_COUNT ELSE 0 END)       AS medium_severity_count,
        0                                                                           AS low_severity_count,

        SUM(CASE WHEN f.SEVERITY = 'HIGH'   THEN f.OFFENSE_COUNT ELSE 0 END) * 1.0 AS weighted_violent,
        SUM(CASE WHEN f.SEVERITY = 'MEDIUM' THEN f.OFFENSE_COUNT ELSE 0 END) * 1.0 AS weighted_property,
        0                                                                           AS drug_related_count,

        SUM(CASE WHEN f.SEVERITY = 'HIGH'   THEN f.OFFENSE_COUNT ELSE 0 END)       AS violent_crime_count,
        SUM(CASE WHEN f.SEVERITY = 'MEDIUM' THEN f.OFFENSE_COUNT ELSE 0 END)       AS property_crime_count,

        0                                                                           AS night_crime_count,
        0                                                                           AS weekend_crime_count,

        NULL                                                                        AS most_common_offense,

        SUM(CASE WHEN f.YEAR = 2024 THEN f.OFFENSE_COUNT ELSE 0 END)               AS incidents_last_12m,
        SUM(CASE WHEN f.YEAR = 2023 THEN f.OFFENSE_COUNT ELSE 0 END)               AS incidents_prior_12m,

        TRUE AS is_aggregate_source

    FROM fbi_crime f
    INNER JOIN master_location ml
        ON UPPER(TRIM(f.CITY)) = UPPER(TRIM(ml.neighborhood_name))
        AND ml.is_greater_boston = TRUE
    GROUP BY 1, 2, 3
),

fbi_top_offense AS (
    SELECT
        CITY,
        UPPER(OFFENSE_TYPE) AS OFFENSE_TYPE,
        SUM(OFFENSE_COUNT) AS total,
        ROW_NUMBER() OVER (PARTITION BY CITY ORDER BY SUM(OFFENSE_COUNT) DESC) AS rn
    FROM fbi_crime
    GROUP BY CITY, UPPER(OFFENSE_TYPE)
),

fbi_final AS (
    SELECT
        fs.location_id,
        fs.neighborhood_name,
        fs.city,
        fs.total_incidents,
        fs.weighted_incidents,
        fs.active_months,
        fs.high_severity_count,
        fs.medium_severity_count,
        fs.low_severity_count,
        fs.weighted_violent,
        fs.weighted_property,
        fs.drug_related_count,
        fs.violent_crime_count,
        fs.property_crime_count,
        fs.night_crime_count,
        fs.weekend_crime_count,
        COALESCE(ft.OFFENSE_TYPE, 'UNKNOWN') AS most_common_offense,
        fs.incidents_last_12m,
        fs.incidents_prior_12m,
        fs.is_aggregate_source
    FROM fbi_city_stats fs
    LEFT JOIN fbi_top_offense ft
        ON UPPER(TRIM(fs.neighborhood_name)) = UPPER(TRIM(ft.CITY))
        AND ft.rn = 1
),

-- ═══════════════════════════════════════════════════════════════════════════
-- MERGE: Combine both tracks, DEDUPLICATE — prefer FBI aggregate for
-- Greater Boston cities over any incident spillover from spatial join
-- ═══════════════════════════════════════════════════════════════════════════

all_stats AS (
    SELECT * FROM incident_stats
    UNION ALL
    SELECT * FROM fbi_final
),

all_stats_deduped AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY location_id
                ORDER BY is_aggregate_source DESC, total_incidents DESC
            ) AS _rn
        FROM all_stats
    )
    WHERE _rn = 1
),

all_locations AS (
    SELECT
        ml.location_id,
        ml.neighborhood_name,
        ml.city,
        ml.sqmiles,
        ml.is_boston,
        ml.is_cambridge,
        ml.is_greater_boston
    FROM master_location ml
),

with_area_full AS (
    SELECT
        al.location_id,
        al.neighborhood_name,
        al.city,
        al.sqmiles,
        al.is_boston,
        al.is_cambridge,
        al.is_greater_boston,
        COALESCE(s.total_incidents,      0)    AS total_incidents,
        COALESCE(s.weighted_incidents,   0)    AS weighted_incidents,
        COALESCE(s.active_months,        0)    AS active_months,
        COALESCE(s.high_severity_count,  0)    AS high_severity_count,
        COALESCE(s.medium_severity_count,0)    AS medium_severity_count,
        COALESCE(s.low_severity_count,   0)    AS low_severity_count,
        COALESCE(s.weighted_violent,     0)    AS weighted_violent,
        COALESCE(s.weighted_property,    0)    AS weighted_property,
        COALESCE(s.drug_related_count,   0)    AS drug_related_count,
        COALESCE(s.violent_crime_count,  0)    AS violent_crime_count,
        COALESCE(s.property_crime_count, 0)    AS property_crime_count,
        COALESCE(s.night_crime_count,    0)    AS night_crime_count,
        COALESCE(s.weekend_crime_count,  0)    AS weekend_crime_count,
        UPPER(s.most_common_offense)           AS most_common_offense,
        COALESCE(s.incidents_last_12m,   0)    AS incidents_last_12m,
        COALESCE(s.incidents_prior_12m,  0)    AS incidents_prior_12m,
        COALESCE(s.is_aggregate_source, FALSE) AS is_aggregate_source
    FROM all_locations al
    LEFT JOIN all_stats_deduped s ON al.location_id = s.location_id
),

with_metrics AS (
    SELECT
        *,
        -- ── Scoring signals ──────────────────────────────────────────────
        -- Denominator for rates: weighted_violent + weighted_property (excludes LOW severity
        -- service calls like SICK ASSIST, INVESTIGATE PERSON). This ensures apples-to-apples
        -- comparison between incident-level cities (which have LOW severity) and FBI aggregate
        -- cities (which only report HIGH+MEDIUM offenses).
        ROUND((weighted_violent + weighted_property) / NULLIF(sqmiles, 0), 2)                       AS crime_density,
        ROUND(weighted_violent   / NULLIF(weighted_violent + weighted_property, 0), 4)               AS violent_rate,
        ROUND(weighted_property  / NULLIF(weighted_violent + weighted_property, 0), 4)               AS property_rate,
        ROUND(high_severity_count * 1.0 / NULLIF(high_severity_count + medium_severity_count, 0), 4) AS high_severity_rate,
        ROUND(night_crime_count * 1.0 / NULLIF(total_incidents, 0), 4)                               AS night_crime_rate,
        CASE
            WHEN incidents_prior_12m >= 50
            THEN ROUND((incidents_last_12m - incidents_prior_12m) * 1.0
                 / NULLIF(incidents_prior_12m, 0), 4)
            ELSE 0
        END AS yoy_trend_rate,

        -- ── Display metrics (all capped to 2 decimal places) ─────────────
        -- Display pct_violent/pct_property use total_incidents as denominator
        -- (shows user the full picture including service calls)
        ROUND(total_incidents    / NULLIF(sqmiles, 0),       2) AS incidents_per_sqmile,
        ROUND(total_incidents    / NULLIF(active_months, 0), 2) AS avg_monthly_incidents,
        ROUND(high_severity_count  * 100.0 / NULLIF(total_incidents, 0), 2) AS pct_high_severity,
        ROUND(violent_crime_count  * 100.0 / NULLIF(total_incidents, 0), 2) AS pct_violent,
        ROUND(property_crime_count * 100.0 / NULLIF(total_incidents, 0), 2) AS pct_property,
        ROUND(drug_related_count   * 100.0 / NULLIF(total_incidents, 0), 2) AS pct_drug_related,
        ROUND(night_crime_count    * 100.0 / NULLIF(total_incidents, 0), 2) AS pct_night_crimes,
        ROUND(weekend_crime_count  * 100.0 / NULLIF(total_incidents, 0), 2) AS pct_weekend_crimes,
        ROUND(
            CASE
                WHEN incidents_prior_12m = 0 THEN NULL
                ELSE (incidents_last_12m - incidents_prior_12m) * 100.0
                     / NULLIF(incidents_prior_12m, 0)
            END,
        2) AS yoy_change_pct,

        CASE WHEN total_incidents < 50 THEN TRUE ELSE FALSE END AS insufficient_data,

        CASE
            WHEN is_boston         = TRUE THEN 'BOSTON'
            WHEN is_cambridge     = TRUE THEN 'CAMBRIDGE'
            WHEN is_greater_boston = TRUE THEN 'GREATER_BOSTON'
            ELSE 'UNKNOWN'
        END AS distribution_group

    FROM with_area_full
),

-- ── PASS 1: Within-group Z-score distributions ────────────────────────────
boston_dist AS (
    SELECT
        AVG(crime_density)       AS mean_density,   STDDEV(crime_density)    AS std_density,
        AVG(violent_rate)        AS mean_violent,    STDDEV(violent_rate)     AS std_violent,
        AVG(property_rate)       AS mean_property,   STDDEV(property_rate)    AS std_property,
        AVG(high_severity_rate)  AS mean_severity,   STDDEV(high_severity_rate) AS std_severity,
        AVG(night_crime_rate)    AS mean_night,      STDDEV(night_crime_rate) AS std_night,
        AVG(yoy_trend_rate)      AS mean_trend,      STDDEV(yoy_trend_rate)   AS std_trend
    FROM with_metrics
    WHERE distribution_group = 'BOSTON' AND insufficient_data = FALSE
),

cambridge_dist AS (
    SELECT
        AVG(crime_density)       AS mean_density,   STDDEV(crime_density)    AS std_density,
        AVG(violent_rate)        AS mean_violent,    STDDEV(violent_rate)     AS std_violent,
        AVG(property_rate)       AS mean_property,   STDDEV(property_rate)    AS std_property,
        AVG(high_severity_rate)  AS mean_severity,   STDDEV(high_severity_rate) AS std_severity,
        AVG(night_crime_rate)    AS mean_night,      STDDEV(night_crime_rate) AS std_night,
        AVG(yoy_trend_rate)      AS mean_trend,      STDDEV(yoy_trend_rate)   AS std_trend
    FROM with_metrics
    WHERE distribution_group = 'CAMBRIDGE' AND insufficient_data = FALSE
),

combined_dist AS (
    SELECT
        AVG(crime_density)       AS mean_density,   STDDEV(crime_density)    AS std_density,
        AVG(violent_rate)        AS mean_violent,    STDDEV(violent_rate)     AS std_violent,
        AVG(property_rate)       AS mean_property,   STDDEV(property_rate)    AS std_property,
        AVG(high_severity_rate)  AS mean_severity,   STDDEV(high_severity_rate) AS std_severity,
        AVG(night_crime_rate)    AS mean_night,      STDDEV(night_crime_rate) AS std_night,
        AVG(yoy_trend_rate)      AS mean_trend,      STDDEV(yoy_trend_rate)   AS std_trend
    FROM with_metrics
    WHERE distribution_group IN ('BOSTON', 'CAMBRIDGE') AND insufficient_data = FALSE
),

with_pass1_scores AS (
    SELECT
        m.*,
        CASE
            WHEN m.insufficient_data = TRUE THEN NULL

            -- Boston: full 6-signal scoring
            WHEN m.distribution_group = 'BOSTON' THEN
                ROUND(LEAST(100, GREATEST(0,
                    100.0
                    - (40.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.violent_rate       - d_b.mean_violent)  / NULLIF(d_b.std_violent,  0) * 0.15)))
                    - (20.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.crime_density      - d_b.mean_density)  / NULLIF(d_b.std_density,  0) * 0.15)))
                    - (15.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.property_rate      - d_b.mean_property) / NULLIF(d_b.std_property, 0) * 0.15)))
                    - (10.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.high_severity_rate - d_b.mean_severity) / NULLIF(d_b.std_severity, 0) * 0.15)))
                    - (10.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.yoy_trend_rate     - d_b.mean_trend)    / NULLIF(d_b.std_trend,    0) * 0.15)))
                    - ( 5.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.night_crime_rate   - d_b.mean_night)    / NULLIF(d_b.std_night,    0) * 0.15)))
                )), 1)

            -- Cambridge: full 6-signal scoring
            WHEN m.distribution_group = 'CAMBRIDGE' THEN
                ROUND(LEAST(100, GREATEST(0,
                    100.0
                    - (40.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.violent_rate       - d_c.mean_violent)  / NULLIF(d_c.std_violent,  0) * 0.15)))
                    - (20.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.crime_density      - d_c.mean_density)  / NULLIF(d_c.std_density,  0) * 0.15)))
                    - (15.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.property_rate      - d_c.mean_property) / NULLIF(d_c.std_property, 0) * 0.15)))
                    - (10.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.high_severity_rate - d_c.mean_severity) / NULLIF(d_c.std_severity, 0) * 0.15)))
                    - (10.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.yoy_trend_rate     - d_c.mean_trend)    / NULLIF(d_c.std_trend,    0) * 0.15)))
                    - ( 5.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.night_crime_rate   - d_c.mean_night)    / NULLIF(d_c.std_night,    0) * 0.15)))
                )), 1)

            -- Greater Boston: 4-signal redistributed scoring against combined distribution
            -- Original: violent 40%, density 20%, property 15%, trend 10% = 85%
            -- Redistributed: 47% + 24% + 18% + 11% = 100%
            WHEN m.distribution_group = 'GREATER_BOSTON' THEN
                ROUND(LEAST(100, GREATEST(0,
                    100.0
                    - (47.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.violent_rate       - d_cb.mean_violent)  / NULLIF(d_cb.std_violent,  0) * 0.15)))
                    - (24.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.crime_density      - d_cb.mean_density)  / NULLIF(d_cb.std_density,  0) * 0.15)))
                    - (18.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.property_rate      - d_cb.mean_property) / NULLIF(d_cb.std_property, 0) * 0.15)))
                    - (11.0 * LEAST(1.0, GREATEST(0.0, 0.5 + (m.yoy_trend_rate     - d_cb.mean_trend)    / NULLIF(d_cb.std_trend,    0) * 0.15)))
                )), 1)

            ELSE NULL
        END AS pass1_score

    FROM with_metrics m
    CROSS JOIN boston_dist    d_b
    CROSS JOIN cambridge_dist d_c
    CROSS JOIN combined_dist  d_cb
),

-- ── PASS 2: Re-normalize across all groups for comparable final score ─────
pass2_dist AS (
    SELECT
        AVG(pass1_score)    AS mean_score,
        STDDEV(pass1_score) AS std_score
    FROM with_pass1_scores
    WHERE insufficient_data = FALSE
),

with_final_score AS (
    SELECT
        p.*,
        CASE
            WHEN p.insufficient_data = TRUE THEN NULL
            ELSE ROUND(LEAST(100, GREATEST(0,
                50.0 + ((p.pass1_score - d2.mean_score) / NULLIF(d2.std_score, 0)) * 15.0
            )), 1)
        END AS safety_score
    FROM with_pass1_scores p
    CROSS JOIN pass2_dist d2
),

with_grade AS (
    SELECT
        *,
        CASE
            WHEN insufficient_data = TRUE THEN 'INSUFFICIENT DATA'
            WHEN safety_score >= 75       THEN 'EXCELLENT'
            WHEN safety_score >= 50       THEN 'GOOD'
            WHEN safety_score >= 25       THEN 'MODERATE'
            ELSE                               'HIGH CONCERN'
        END AS safety_grade
    FROM with_final_score
),

with_description AS (
    SELECT
        location_id,
        UPPER(neighborhood_name)    AS neighborhood_name,
        UPPER(city)                 AS city,
        sqmiles,
        UPPER(distribution_group)   AS distribution_group,
        is_aggregate_source,
        total_incidents,
        incidents_per_sqmile,
        avg_monthly_incidents,
        high_severity_count,
        medium_severity_count,
        low_severity_count,
        pct_high_severity,
        violent_crime_count,
        property_crime_count,
        drug_related_count,
        pct_violent,
        pct_property,
        pct_drug_related,
        night_crime_count,
        weekend_crime_count,
        pct_night_crimes,
        pct_weekend_crimes,
        UPPER(most_common_offense)  AS most_common_offense,
        incidents_last_12m,
        incidents_prior_12m,
        yoy_change_pct,
        insufficient_data,
        pass1_score,
        safety_score,
        UPPER(safety_grade)         AS safety_grade,

        CASE
            WHEN insufficient_data = TRUE
            THEN 'Insufficient crime data available for ' || UPPER(neighborhood_name)
                 || '. This area has fewer than 50 recorded incidents in the dataset.'
            ELSE SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large',
                'You are a safety analyst writing for a neighborhood guide covering '
                || UPPER(city) || ', Massachusetts. '
                || 'Write a concise 2-3 sentence safety summary for ' || UPPER(neighborhood_name)
                || ', located in ' || UPPER(city) || ', Massachusetts. '
                || 'IMPORTANT: ' || UPPER(neighborhood_name)
                || ' is in ' || UPPER(city) || ', Massachusetts. '
                || 'Do not confuse it with any place of the same name elsewhere. '
                || 'Do not use bullet points. Write in plain prose only. '
                || 'Be specific about crime patterns and the most prevalent crime types. '
                || 'NOTE: Offense types like SICK ASSIST, INVESTIGATE PERSON, and INVESTIGATE PROPERTY '
                || 'are police service calls, not criminal offenses. If the most common offense is a service call, '
                || 'note that and mention the most prevalent actual crime type instead. '
                || CASE
                    WHEN is_aggregate_source = TRUE
                    THEN 'NOTE: Crime data for this city comes from FBI annual reports (2022-2024) rather than '
                         || 'real-time incident data. Night and weekend crime breakdowns are not available for this location. '
                         || 'Focus your summary on the violent vs property crime split and year-over-year trends. '
                    ELSE ''
                END
                || 'Safety score: ' || ROUND(safety_score, 1) || '/100 (' || safety_grade || '). '
                || 'Total incidents: ' || total_incidents || '. '
                || 'Incidents per sq mile: ' || incidents_per_sqmile
                || CASE WHEN sqmiles < 0.2
                    THEN ' (note: this is a very small area under 0.2 sq miles — per-sq-mile rates appear inflated). '
                    ELSE '. '
                END
                || 'Violent crime: ' || pct_violent || '% of incidents. '
                || 'Property crime: ' || pct_property || '% of incidents. '
                || 'High severity: ' || pct_high_severity || '% of incidents. '
                || 'Most common offense: ' || COALESCE(UPPER(most_common_offense), 'UNKNOWN') || '. '
                || 'Year over year change: ' || COALESCE(
                    CASE
                        WHEN yoy_change_pct > 0 THEN '+' || yoy_change_pct || '% (increasing)'
                        WHEN yoy_change_pct < 0 THEN yoy_change_pct || '% (decreasing)'
                        ELSE 'No significant change'
                    END,
                    'Insufficient baseline data for trend'
                ) || '.'
            )
        END AS safety_description,

        TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS load_timestamp

    FROM with_grade
)

SELECT *
FROM with_description
ORDER BY safety_score DESC NULLS LAST