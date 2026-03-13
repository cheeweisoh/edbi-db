{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['case_pid', 'officer_id', '_file_date'],
    partition_by=['_file_date'],
    on_schema_change='fail',
    tags=['silver', 'qa']
)}}

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
        CASE WHEN officer_name IS NULL THEN false ELSE true END AS _dq_missing_officer_name
    FROM {{ ref('cmplx_criliti_sc_assigned_lo') }}
    WHERE _rejected_reason IS NULL
    {% if is_incremental() %}
        AND _bronze_loaded_at > (SELECT MAX(_bronze_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    *,
    CASE
        WHEN _dq_missing_officer_name = true THEN true
        ELSE false
    END AS is_valid_row
FROM base
