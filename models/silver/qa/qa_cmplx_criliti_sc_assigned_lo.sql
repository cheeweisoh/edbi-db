WITH base AS (
    SELECT 
        case_pid,
        cluster,
        officer_name,
        officer_id,
        team,
        _rescued_data,
        _source_file,
        _file_date,
        _ingested_at,
        _bronze_loaded_at,

        -- quality flags
        CASE WHEN officer_name IS NULL THEN true ELSE false END AS _dq_missing_officer_name
    FROM {{ source('bronze', 'cmplx_criliti_sc_assigned_lo') }}
    WHERE _rejected_reason IS NULL
)

SELECT
    *,
    CASE
        WHEN _dq_missing_officer_name = true THEN true
        ELSE false
    END AS is_valid_row
FROM base
