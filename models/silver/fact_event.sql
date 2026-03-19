{{ config(
    materialized='incremental',
    unique_key='event_skey',
    incremental_strategy='merge',
    tags=['silver']
) }}

WITH court_events AS (
    SELECT *
    FROM {{ ref('qa_tb_criliti_sc_court_events') }}
    WHERE is_valid_row = TRUE
    {% if is_incremental() %}
        AND _file_date > (SELECT MAX(_file_date) FROM {{ this }})
    {% endif %}
),

court_event_off AS (
    SELECT *
    FROM {{ ref('qa_tb_criliti_sc_court_event_off') }}
    WHERE is_valid_row = TRUE
),

combined AS (
    SELECT
        court_events.court_event_id,
        court_events.case_pid,
        court_event_off.officer_id,
        court_events.court_event_type,
        court_events.start_datetime,
        court_events.end_datetime,
        DATE_TRUNC('day', court_events.start_datetime) AS event_date,
        court_events._file_date,
        court_events._bronze_loaded_at
    FROM court_events
    INNER JOIN court_event_off
        ON court_events.court_event_id = court_event_off.court_event_id
),

deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY court_event_id, officer_id
                ORDER BY _file_date DESC
            ) AS rn
        FROM combined
    )
    WHERE rn = 1
),

fact_event_source AS (
    SELECT
        court_event_id,
        case_pid,
        officer_id,
        court_event_type,
        event_date,
        CASE
            WHEN court_event_type IN ('CC', 'PTC') THEN 12.5 / 510.0
            WHEN court_event_type IN ('TRIAL', 'PH') THEN
                CASE
                    WHEN EXTRACT(HOUR FROM start_datetime) >= 13 THEN 0.5
                    ELSE 1.0
                END
            ELSE 85.0 / 510.0
        END AS court_event_hearing_days,
        EXTRACT(YEAR FROM event_date) AS court_event_year,
        _file_date,
        _bronze_loaded_at
    FROM deduplicated
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['court_event_id', 'officer_id']) }} AS event_skey,
    cases.case_skey,
    officers.officer_skey,
    s.court_event_type,
    dates.date_skey AS court_event_date_skey,
    s.court_event_hearing_days,
    s.court_event_year,
    s._file_date,
    s._bronze_loaded_at
FROM fact_event_source s
LEFT JOIN {{ ref('dim_case') }} cases
    ON s.case_pid = cases.case_pid
LEFT JOIN {{ ref('dim_officer') }} officers
    ON s.officer_id = officers.officer_id
LEFT JOIN {{ ref('dim_date') }} dates
    ON CAST(s.event_date AS DATE) = dates.full_date
