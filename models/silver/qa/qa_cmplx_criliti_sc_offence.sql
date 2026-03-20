{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['case_pid', 'charge_no', 'act_title', 'reference', 'rwpu_base', 'description', '_file_date'],
    partition_by=['_file_date'],
    on_schema_change='fail',
    tags=['silver', 'qa']
)}}

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
        current_timestamp() AS _silver_qa_loaded_at,

        -- quality flags
        CASE WHEN rwpu_base NOT IN ('M', 'P', 'R') THEN false ELSE true END AS _dq_invalid_rwpu,
        CASE WHEN _rescued_data IS NOT NULL THEN false ELSE true END AS _dq_rescued_data
    FROM {{ ref('cmplx_criliti_sc_offence') }}
    WHERE _rejected_reason IS NULL
    {% if is_incremental() %}
        AND _bronze_loaded_at > (SELECT COALESCE(MAX(_bronze_loaded_at), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    *,
    CASE
        WHEN _dq_invalid_rwpu = true THEN true
        WHEN _dq_rescued_data = true THEN true
        ELSE false
    END AS is_valid_row
FROM base
