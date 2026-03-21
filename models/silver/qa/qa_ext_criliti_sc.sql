{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['case_pid', '_file_date'],
    partition_by=['_file_date'],
    on_schema_change='fail',
    tags=['silver', 'qa']
)}}

WITH
{% if is_incremental() %}
max_loaded_at AS (
    SELECT COALESCE(MAX(_bronze_loaded_at), TIMESTAMP('1900-01-01')) AS cutoff_bronze_loaded_at
    FROM {{ this }}
),
{% endif %}

base AS (
    SELECT
        accused_name,
        case_no,
        case_pid,
        UPPER(TRIM(case_status)) AS case_status,
        case_title,
        UPPER(TRIM(directorate)) AS directorate,
        TRY_CAST(accused_dob AS DATE) AS accused_dob,
        accused_gender,
        _rescued_data,
        _source_file,
        _file_date,
        _ingested_at,
        _bronze_loaded_at,
        current_timestamp() AS _silver_qa_loaded_at,

        -- quality flags
        CASE WHEN case_no IS NULL THEN false ELSE true END AS _dq_missing_case_no,
        CASE WHEN (accused_dob IS NULL) OR (accused_dob > CURRENT_DATE) THEN false ELSE true END AS _dq_future_accused_dob,
        CASE WHEN accused_gender NOT IN ('M', 'F') THEN false ELSE true END AS _dq_invalid_gender,
        CASE WHEN _rescued_data IS NOT NULL THEN false ELSE true END AS _dq_rescued_data
    FROM {{ ref('ext_criliti_sc') }} src
    {% if is_incremental() %}
    CROSS JOIN max_loaded_at
    {% endif %}
    WHERE _rejected_reason IS NULL
    {% if is_incremental() %}
        AND src._bronze_loaded_at > max_loaded_at.cutoff_bronze_loaded_at
    {% endif %}
)

SELECT
    *,
    CASE
        WHEN _dq_missing_case_no = true THEN true
        WHEN _dq_future_accused_dob = true THEN true
        WHEN _dq_invalid_gender = true THEN true
        WHEN _dq_rescued_data = true THEN true
        ELSE false
    END AS is_valid_row
FROM base
