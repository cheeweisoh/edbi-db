{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['case_pid', 'charge_no', '_file_date'],
    partition_by=['_file_date'],
    on_schema_change='fail',
    tags=['silver', 'qa']
)}}

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
        CASE WHEN charge_type NOT IN ('DAC', 'MAC', 'MCN') THEN false ELSE true END AS _dq_invalid_charge_type,
        CASE WHEN offence_date > CURRENT_DATE THEN false ELSE true END AS _dq_future_offence_date,
        CASE WHEN filing_date > CURRENT_DATE THEN false ELSE true END AS _dq_future_filing_date,
        CASE WHEN offence_date > filing_date THEN false ELSE true END AS _dq_filing_before_offence
    FROM {{ ref('cmplx_criliti_sc_charge_dtls') }}
    WHERE _rejected_reason IS NULL
    {% if is_incremental() %}
        AND _bronze_loaded_at > (SELECT MAX(_bronze_loaded_at) FROM {{ this }})
    {% endif %}
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
