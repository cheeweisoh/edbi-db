{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['case_pid', 'court_event_id', '_file_date'],
    partition_by=['_file_date'],
    on_schema_change='fail',
    tags=['silver', 'qa']
)}}

WITH base AS (
    SELECT
        start_datetime,
        end_datetime, 
        case_pid,
        court_event_id,
        court_number,
        court_event_type,
        court_event_status,
        _rescued_data,
        _source_file,
        _file_date,
        _ingested_at,
        _bronze_loaded_at,
        current_timestamp() AS _silver_qa_loaded_at,

        -- quality flags
        CASE WHEN (EXTRACT(HOUR FROM start_datetime) BETWEEN 0 AND 7) OR (EXTRACT(HOUR FROM start_datetime) BETWEEN 19 AND 23) THEN false ELSE true END AS _dq_early_start_datetime,
        CASE WHEN end_datetime < start_datetime THEN false ELSE true END AS _dq_end_before_start,
        CASE WHEN court_number IS NULL THEN false ELSE true END AS _dq_missing_court_number,
        CASE WHEN court_event_type NOT IN ('FM', 'FM_PG', 'PG', 'CC', 'PTC', 'PH', 'SENTENCING', 'TRIAL', 'MITIGATION') THEN false ELSE true END AS _dq_invalid_court_event_type,
        CASE WHEN court_event_status NOT IN ('NEW', 'VACATED', 'VOIDED') THEN false ELSE true END AS _dq_invalid_court_event_status
    FROM {{ ref('tb_criliti_sc_court_events') }}
    WHERE _rejected_reason IS NULL
    {% if is_incremental() %}
        AND _bronze_loaded_at > (SELECT MAX(_bronze_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    *,
    CASE
        WHEN _dq_early_start_datetime = true THEN true
        WHEN _dq_future_end_datetime = true THEN true
        WHEN _dq_missing_court_number = true THEN true
        WHEN _dq_invalid_court_event_type = true THEN true
        WHEN _dq_invalid_court_event_status = true THEN true
        ELSE false
    END AS is_valid_row
       
