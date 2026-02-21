{{ config(
    materialized = 'table'
) }}

with src as (

    select *
    from {{ source('healthcare_stage', 'STG_BOSTON_HEALTHCARE') }}

),

/* ------------------------------------------------------------------
   1) Normalize/clean raw strings first (NO alias re-use in same SELECT)
------------------------------------------------------------------- */
base_raw as (

    select
        /* ---------- ID ---------- */
        nullif(trim(DPH_FACILITY_ID), '')                                         as dph_facility_id,

        /* ---------- facility type: raw + canonical + grouped ---------- */
        nullif(trim(FACILITY_TYPE), '')                                           as facility_type_raw,

        case
            when trim(FACILITY_TYPE) ilike 'Ambulatory Surgical Ctr.%'             then 'Ambulatory Surgical Center'
            when trim(FACILITY_TYPE) ilike 'Outpt.%Phys.%Ther%/Speech%Path.%'      then 'Outpatient PT / Speech Pathology'
            when trim(FACILITY_TYPE) ilike 'Renal Dialysis (ESRD)%'                then 'Renal Dialysis (ESRD)'
            when trim(FACILITY_TYPE) ilike 'Temp.%Nursing Agency%'                 then 'Temporary Nursing Agency'
            when trim(FACILITY_TYPE) ilike 'Mob/Port Hospital Satellite%'          then 'Mobile / Portable Hospital Satellite'
            when trim(FACILITY_TYPE) ilike 'Mobile/Portable Clinic Satellite%'     then 'Mobile / Portable Clinic Satellite'
            when trim(FACILITY_TYPE) ilike 'Mobile/Portable Clinic%'               then 'Mobile / Portable Clinic'
            else initcap(trim(FACILITY_TYPE))
        end                                                                       as facility_type_canonical,

        case
            when trim(FACILITY_TYPE) ilike '%Hospital%' or trim(FACILITY_TYPE) ilike '%Inpatient%' then 'Inpatient / Hospital'
            when trim(FACILITY_TYPE) ilike '%Clinic%' or trim(FACILITY_TYPE) ilike '%Ambulatory%'  then 'Outpatient / Clinic'
            when trim(FACILITY_TYPE) ilike '%Home Health%'                                          then 'Home Health'
            when trim(FACILITY_TYPE) ilike '%Hospice%'                                              then 'Hospice'
            when trim(FACILITY_TYPE) ilike '%Nursing Home%' or trim(FACILITY_TYPE) ilike '%Rest Home%' then 'Long-Term Care'
            when trim(FACILITY_TYPE) ilike '%Adult Day Health%'                                     then 'Adult Day Health'
            when trim(FACILITY_TYPE) ilike '%Renal Dialysis%'                                       then 'Dialysis'
            when trim(FACILITY_TYPE) ilike '%Agency%'                                               then 'Agency / Staffing'
            when trim(FACILITY_TYPE) ilike '%Research%'                                             then 'Research'
            else 'Other'
        end                                                                       as facility_type_group,

        /* ---------- name + address cleanup ---------- */
        nullif(regexp_replace(trim(FACILITY_NAME), '\\s+', ' '), '')               as facility_name_clean,
        nullif(regexp_replace(trim(STREET), '\\s+', ' '), '')                      as street_clean,
        nullif(regexp_replace(trim(CITY_TOWN), '\\s+', ' '), '')                   as city_clean,

        /* ZIP: digits-only + pad-to-5 (fixes 1902 -> 01902, etc.) */
        nullif(
            lpad(regexp_replace(coalesce(ZIP_CODE::string, ''), '[^0-9]', ''), 5, '0'),
            ''
        )                                                                          as zip_code_5,

        /* ---------- phone cleanup: raw + digits ---------- */
        nullif(trim(TELEPHONE), '')                                                as telephone_raw,
        regexp_replace(coalesce(TELEPHONE::string, ''), '[^0-9]', '')              as phone_digits_all,

        /* ---------- numeric cleaning (handles Not Applicable / N/A) ---------- */
        case
            when BED_COUNT is null then null
            when upper(trim(BED_COUNT)) in ('NOT APPLICABLE','N/A','') then null
            else try_to_number(BED_COUNT)
        end                                                                        as bed_count,

        case
            when ADULT_DAY_HEALTH_CAPACITY is null then null
            when upper(trim(ADULT_DAY_HEALTH_CAPACITY)) in ('NOT APPLICABLE','N/A','') then null
            else try_to_number(ADULT_DAY_HEALTH_CAPACITY)
        end                                                                        as adult_day_health_capacity,

        /* ---------- timestamps ---------- */
        LOAD_TIMESTAMP::timestamp_ntz                                              as load_timestamp_raw,
        current_timestamp()                                                       as transformed_at,
        coalesce(LOAD_TIMESTAMP::timestamp_ntz, current_timestamp())              as effective_load_ts

    from src
),

/* ------------------------------------------------------------------
   2) Derive fields that depend on earlier aliases (zip/phone)
------------------------------------------------------------------- */
base as (

    select
        br.*,

        /* infer MA from ZIP numeric range 01000–02799 */
        case
            when try_to_number(br.zip_code_5) between 1000 and 2799 then true
            else false
        end                                                                        as is_ma_zip,

        /* Extension (supports: ext 123, ext.123, x123, x 123) */
        regexp_substr(
        lower(coalesce(br.telephone_raw, '')),
        '(ext|x)[ ]*([0-9]{1,6})',1, 1, 'e', 2) as phone_extension,

        /* main 10 digits (if 11 digits with leading 1, we still take first 10 after the 1) */
        case
            when length(br.phone_digits_all) = 10 then br.phone_digits_all
            when length(br.phone_digits_all) = 11 and left(br.phone_digits_all, 1) = '1'
                then substr(br.phone_digits_all, 2, 10)
            when length(br.phone_digits_all) >= 10
                then substr(br.phone_digits_all, 1, 10)
            else null
        end                                                                        as phone_main_10,

        /* E.164 */
        case
            when length(br.phone_digits_all) = 10 then '+1' || br.phone_digits_all
            when length(br.phone_digits_all) = 11 and left(br.phone_digits_all, 1) = '1' then '+' || br.phone_digits_all
            when (
                case
                    when length(br.phone_digits_all) = 10 then br.phone_digits_all
                    when length(br.phone_digits_all) = 11 and left(br.phone_digits_all, 1) = '1' then substr(br.phone_digits_all, 2, 10)
                    when length(br.phone_digits_all) >= 10 then substr(br.phone_digits_all, 1, 10)
                    else null
                end
            ) is not null
                then '+1' || (
                    case
                        when length(br.phone_digits_all) = 10 then br.phone_digits_all
                        when length(br.phone_digits_all) = 11 and left(br.phone_digits_all, 1) = '1' then substr(br.phone_digits_all, 2, 10)
                        when length(br.phone_digits_all) >= 10 then substr(br.phone_digits_all, 1, 10)
                        else null
                    end
                )
            else null
        end                                                                        as phone_e164,

        case when
            case
                when length(br.phone_digits_all) = 10 then '+1' || br.phone_digits_all
                when length(br.phone_digits_all) = 11 and left(br.phone_digits_all, 1) = '1' then '+' || br.phone_digits_all
                when (
                    case
                        when length(br.phone_digits_all) = 10 then br.phone_digits_all
                        when length(br.phone_digits_all) = 11 and left(br.phone_digits_all, 1) = '1' then substr(br.phone_digits_all, 2, 10)
                        when length(br.phone_digits_all) >= 10 then substr(br.phone_digits_all, 1, 10)
                        else null
                    end
                ) is not null
                    then '+1' || (
                        case
                            when length(br.phone_digits_all) = 10 then br.phone_digits_all
                            when length(br.phone_digits_all) = 11 and left(br.phone_digits_all, 1) = '1' then substr(br.phone_digits_all, 2, 10)
                            when length(br.phone_digits_all) >= 10 then substr(br.phone_digits_all, 1, 10)
                            else null
                        end
                    )
                else null
            end
        is not null then true else false end                                       as has_valid_phone

    from base_raw br
),

derived as (

    select
        b.*,

        case when b.is_ma_zip then 'MA' else null end                              as state_inferred,

        concat_ws(', ',
            b.street_clean,
            b.city_clean,
            case when b.is_ma_zip then 'MA ' || b.zip_code_5 else b.zip_code_5 end
        )                                                                          as full_address,

        lower(regexp_replace(coalesce(b.facility_name_clean, ''), '[^a-zA-Z0-9 ]', '')) as facility_name_norm,
        lower(regexp_replace(coalesce(
            concat_ws(', ',
                b.street_clean,
                b.city_clean,
                case when b.is_ma_zip then 'MA ' || b.zip_code_5 else b.zip_code_5 end
            ), ''
        ), '[^a-zA-Z0-9 ]', ''))                                                   as address_norm,

        {{ dbt_utils.generate_surrogate_key([
            'dph_facility_id',
            'facility_type_raw',
            'facility_name_clean',
            'street_clean',
            'city_clean',
            'zip_code_5',
            'telephone_raw',
            'bed_count',
            'adult_day_health_capacity'
        ]) }}                                                                      as row_hash,

        (
            25 * (case when b.facility_name_clean is not null then 1 else 0 end) +
            20 * (case when b.street_clean is not null then 1 else 0 end) +
            15 * (case when b.city_clean is not null then 1 else 0 end) +
            15 * (case when b.zip_code_5 is not null then 1 else 0 end) +
            15 * (case when b.has_valid_phone then 1 else 0 end) +
            10 * (case when b.facility_type_canonical is not null then 1 else 0 end)
        )                                                                          as quality_score,

        case
            when (
                25 * (case when b.facility_name_clean is not null then 1 else 0 end) +
                20 * (case when b.street_clean is not null then 1 else 0 end) +
                15 * (case when b.city_clean is not null then 1 else 0 end) +
                15 * (case when b.zip_code_5 is not null then 1 else 0 end) +
                15 * (case when b.has_valid_phone then 1 else 0 end) +
                10 * (case when b.facility_type_canonical is not null then 1 else 0 end)
            ) >= 85 then 'HIGH'
            when (
                25 * (case when b.facility_name_clean is not null then 1 else 0 end) +
                20 * (case when b.street_clean is not null then 1 else 0 end) +
                15 * (case when b.city_clean is not null then 1 else 0 end) +
                15 * (case when b.zip_code_5 is not null then 1 else 0 end) +
                15 * (case when b.has_valid_phone then 1 else 0 end) +
                10 * (case when b.facility_type_canonical is not null then 1 else 0 end)
            ) >= 60 then 'MEDIUM'
            else 'LOW'
        end                                                                        as quality_tier,

        concat_ws(
            ' | ',
            b.facility_name_clean,
            b.facility_type_canonical,
            concat_ws(', ',
                b.street_clean,
                b.city_clean,
                case when b.is_ma_zip then 'MA ' || b.zip_code_5 else b.zip_code_5 end
            ),
            case when b.phone_e164 is not null then 'Phone ' || b.phone_e164 else null end,
            case when b.bed_count is not null then 'Beds ' || b.bed_count::string else null end,
            case when b.adult_day_health_capacity is not null then 'Adult day capacity ' || b.adult_day_health_capacity::string else null end
        )                                                                          as search_text,

        concat_ws(
            ' ',
            b.facility_name_clean || ' is a ' || coalesce(b.facility_type_canonical, 'healthcare facility') || '.',
            case
                when b.street_clean is not null or b.city_clean is not null or b.zip_code_5 is not null
                    then 'Located at ' || concat_ws(', ',
                        b.street_clean,
                        b.city_clean,
                        case when b.is_ma_zip then 'MA ' || b.zip_code_5 else b.zip_code_5 end
                    ) || '.'
                else null
            end,
            case when b.bed_count is not null then 'Licensed bed count: ' || b.bed_count::string || '.' else null end,
            case when b.adult_day_health_capacity is not null then 'Adult day health capacity: ' || b.adult_day_health_capacity::string || '.' else null end,
            case when b.phone_e164 is not null then 'Contact: ' || b.phone_e164 || '.' else null end
        )                                                                          as embedding_text,

        object_construct_keep_null(
            'dph_facility_id', b.dph_facility_id,
            'facility_name', b.facility_name_clean,
            'facility_type_raw', b.facility_type_raw,
            'facility_type_canonical', b.facility_type_canonical,
            'facility_type_group', b.facility_type_group,
            'street', b.street_clean,
            'city', b.city_clean,
            'zip', b.zip_code_5,
            'state_inferred', case when b.is_ma_zip then 'MA' else null end,
            'is_ma_zip', b.is_ma_zip,
            'phone_raw', b.telephone_raw,
            'phone_digits', b.phone_digits_all,
            'phone_e164', b.phone_e164,
            'phone_extension', b.phone_extension,
            'bed_count', b.bed_count,
            'adult_day_health_capacity', b.adult_day_health_capacity,
            'quality_score', (
                25 * (case when b.facility_name_clean is not null then 1 else 0 end) +
                20 * (case when b.street_clean is not null then 1 else 0 end) +
                15 * (case when b.city_clean is not null then 1 else 0 end) +
                15 * (case when b.zip_code_5 is not null then 1 else 0 end) +
                15 * (case when b.has_valid_phone then 1 else 0 end) +
                10 * (case when b.facility_type_canonical is not null then 1 else 0 end)
            ),
            'quality_tier', case
                when (
                    25 * (case when b.facility_name_clean is not null then 1 else 0 end) +
                    20 * (case when b.street_clean is not null then 1 else 0 end) +
                    15 * (case when b.city_clean is not null then 1 else 0 end) +
                    15 * (case when b.zip_code_5 is not null then 1 else 0 end) +
                    15 * (case when b.has_valid_phone then 1 else 0 end) +
                    10 * (case when b.facility_type_canonical is not null then 1 else 0 end)
                ) >= 85 then 'HIGH'
                when (
                    25 * (case when b.facility_name_clean is not null then 1 else 0 end) +
                    20 * (case when b.street_clean is not null then 1 else 0 end) +
                    15 * (case when b.city_clean is not null then 1 else 0 end) +
                    15 * (case when b.zip_code_5 is not null then 1 else 0 end) +
                    15 * (case when b.has_valid_phone then 1 else 0 end) +
                    10 * (case when b.facility_type_canonical is not null then 1 else 0 end)
                ) >= 60 then 'MEDIUM'
                else 'LOW'
            end,
            'row_hash', {{ dbt_utils.generate_surrogate_key([
                'dph_facility_id',
                'facility_type_raw',
                'facility_name_clean',
                'street_clean',
                'city_clean',
                'zip_code_5',
                'telephone_raw',
                'bed_count',
                'adult_day_health_capacity'
            ]) }},
            'effective_load_ts', b.effective_load_ts
        )                                                                          as profile_json

    from base b
)

select *
from derived
where dph_facility_id is not null
