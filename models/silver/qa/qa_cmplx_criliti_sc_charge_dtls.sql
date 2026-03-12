WITH base AS (
    SELECT
        case_pid,
        charge_no,
        charge_type,
        charge_status,
        offence_date,
        filing_date,
        _rescued_data,
        _source_file,
        _file_date,
        _ingested_at,
        _bronze_loaded_at,

        -- quality flags
        CASE WHEN charge_type NOT IN ('DAC', 'MAC', 'MCN') THEN true ELSE false END AS _dq_invalid_charge_type,
        CASE WHEN offence_date > CURRENT_DATE THEN true ELSE false END AS _dq_future_offence_date,
        CASE WHEN filing_date > CURRENT_DATE THEN true ELSE false END AS _dq_future_filing_date,
        CASE WHEN offence_date > filing_date THEN true ELSE false END AS _dq_filing_before_offence
    FROM {{ source('bronze', 'cmplx_criliti_sc_charge_dtls') }}
    WHERE _rejected_reason IS NULL
)

SELECT
    *,
    CASE
        WHEN _dq_invalid_charge_type = true THEN true
        WHEN _dq_future_offence_date = true THEN true
        WHEN _dq_future_filing_date = true THEN true
        WHEN _dq_filing_before_offence = true THEN true
        ELSE false
    END AS is_valid_row
FROM base
