WITH base AS (
    SELECT
        case_pid,
        charge_no,
        act_title,
        reference,
        rwpu_base,
        description,
        _rescued_data,
        _source_file,
        _file_date,
        _ingested_at,
        _bronze_loaded_at,

        -- quality flags
        CASE WHEN rwpu_base NOT IN ('M', 'P', 'R') THEN true ELSE false END AS _dq_invalid_rwpu
    FROM {{ source('bronze', 'cmplx_criliti_sc_offence') }}
    WHERE _rejected_reason IS NULL
)

SELECT
    *,
    CASE
        WHEN _dq_invalid_rwpu = true THEN true
        ELSE false
    END AS is_valid_row
FROM base
