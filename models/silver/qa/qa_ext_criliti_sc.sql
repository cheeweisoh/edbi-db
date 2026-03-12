{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['case_pid', '_file_date'],
    partition_by=['_file_date'],
    on_schema_change='fail',
    tags=['silver', 'qa']
)}}

WITH base AS (
    SELECT
        accused_name,
        case_no,
        case_pid,
        UPPER(TRIM(case_status)) AS case_status,
        case_title,
        UPPER(TRIM(directorate)) AS directorate,
        accused_dob,
        accused_gender,
        _rescued_data,
        _source_file,
        _file_date,
        _ingested_at,
        _bronze_loaded_at,

        -- quality flags
        CASE WHEN accused_dob > CURRENT_DATE THEN true ELSE false END AS _dq_future_accused_dob,
        CASE WHEN accused_gender NOT IN ('M', 'F') THEN true ELSE false END AS _dq_invalid_gender
    FROM {{ ref('ext_criliti_sc') }}
    WHERE _rejected_reason IS NULL
    {% if is_incremental() %}
        AND _bronze_loaded_at > (SELECT MAX(_bronze_loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    *,
    CASE
        WHEN _dq_future_accused_dob = true THEN true
        WHEN _dq_invalid_gender = true THEN true
        ELSE false
    END AS is_valid_row
FROM base
