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

        -- quality flags
        CASE WHEN EXTRACT(HOUR FROM start_datetime) BETWEEN 0 AND 7 THEN true ELSE false END AS _dq_early_start_datetime,
        CASE WHEN end_datetime < start_datetime THEN true ELSE false END AS _dq_future_end_datetime,
        CASE WHEN court_number IS NULL THEN true ELSE false END AS _dq_missing_court_number,
        CASE WHEN court_event_type NOT IN ('FM', 'FM_PG', 'PG', 'CC', 'PTC', 'PH', 'SENTENCING', 'TRIAL', 'MITIGATION') THEN true ELSE false END AS _dq_invalid_court_event_type,
        CASE WHEN court_event_status NOT IN ('NEW', 'VACATED', 'VOIDED') THEN true ELSE false END AS _dq_invalid_court_event_status
    FROM {{ source('bronze', 'tb_criliti_sc_court_events') }}
    WHERE _rejected_reason IS NULL
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
       
