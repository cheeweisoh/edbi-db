{{ config(
    materialized='incremental',
    partition_by=['court_event_year'],
    unique_key='event_officer_skey',
    incremental_strategy='delete+insert',
    tags=['silver']
) }}

WITH
{% if is_incremental() %}
max_loaded_at AS (
    SELECT COALESCE(MAX(_bronze_loaded_at), TIMESTAMP('1900-01-01')) AS cutoff_bronze_loaded_at
    FROM {{ this }}
),

{% endif %}
court_events AS (
    SELECT *
    FROM {{ ref('qa_tb_criliti_sc_court_events') }} events
    {% if is_incremental() %}
    CROSS JOIN max_loaded_at
    {% endif %}
    WHERE court_event_status = "NEW"
        AND is_valid_row = TRUE
    {% if is_incremental() %}
        AND events._bronze_loaded_at > max_loaded_at.cutoff_bronze_loaded_at
    {% endif %}
),

court_event_off AS (
    SELECT *
    FROM {{ ref('qa_tb_criliti_sc_court_event_off') }} event_off
    {% if is_incremental() %}
    CROSS JOIN max_loaded_at
    {% endif %}
    WHERE is_valid_row = TRUE
    {% if is_incremental() %}
        AND event_off._bronze_loaded_at > max_loaded_at.cutoff_bronze_loaded_at
    {% endif %}
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

fact_event_source AS (
    SELECT
        s.court_event_id,
        cases.case_skey,
        s.officer_id,
        officers.officer_skey,
        s.court_event_type,
        dates.date_skey AS court_event_date_skey,
        CASE
            WHEN s.court_event_type IN ('CC', 'PTC') THEN 12.5 / 510.0
            WHEN s.court_event_type IN ('TRIAL', 'PH') THEN
                CASE
                    WHEN EXTRACT(HOUR FROM s.start_datetime) >= 13 THEN 0.5
                    ELSE 1.0
                END
            ELSE 85.0 / 510.0
        END AS court_event_hearing_days,
        EXTRACT(YEAR FROM s.event_date) AS court_event_year,
        s._file_date,
        s._bronze_loaded_at
    FROM combined s
    LEFT JOIN {{ ref('dim_case') }} cases
        ON s.case_pid = cases.case_pid
    LEFT JOIN {{ ref('dim_officer') }} officers
        ON s.officer_id = officers.officer_id
    LEFT JOIN {{ ref('dim_date') }} dates
        ON CAST(s.event_date AS DATE) = dates.full_date
),

fact_event_deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY court_event_id, officer_id
                ORDER BY _file_date DESC
            ) AS rn
        FROM fact_event_source
    )
    WHERE rn = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['court_event_id', 'officer_id']) }} AS event_officer_skey,
    case_skey,
    court_event_id,
    officer_skey,
    court_event_type,
    court_event_date_skey,
    court_event_hearing_days,
    court_event_year,
    _file_date,
    _bronze_loaded_at,
    current_timestamp() AS _silver_loaded_at
FROM fact_event_deduplicated
