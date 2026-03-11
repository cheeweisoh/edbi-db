WITH base AS (
    SELECT
        court_event_id,
        officer_id,
        officer_name,
        officer_division,
        _rescued_data,
        _source_file,
        _file_date,
        _ingested_at,
        _bronze_loaded_at,

        -- quality flags
        CASE WHEN officer_name IS NULL THEN true ELSE false AS _dq_missing_officer_name
    FROM {{ source('bronze', 'tb_criliti_sc_court_event_off') }}
    WHERE _rejected_reason IS NULL
)

SELECT
    *,
    CASE
        WHEN _dq_missing_officer_name = true THEN true
        ELSE false
    END AS is_valid_row
FROM base
