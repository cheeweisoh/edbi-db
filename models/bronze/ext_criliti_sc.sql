{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partition_by='_file_date',
    on_schema_change='fail'
) }}

WITH source AS (
    SELECT 
        accusedname AS accused_name,
        caseno AS case_no,
        casepid AS case_pid,
        case_status AS case_status,
        case_title AS case_title,
        directorate AS directorate,
        division AS division,
        dob AS accused_dob,
        gender AS accused_gender,
        Q_IODETAILS AS io_details,
        _rescued_data AS _rescued_data,
        _source_file AS _source_file,
        _ingested_at AS _ingested_at,
        _file_date AS _file_date
    FROM {{ source('landing', 'ext_criliti_sc') }}

    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

validated AS (
    SELECT *
    FROM source
    WHERE case_no IS NOT NULL
        AND case_pid IS NOT NULL
        AND _file_date IS NOT NULL
        AND _file_date != ''
        AND LENGTH(_file_date) = 8
        AND _ingested_at IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY case_no, _file_date
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM validated
)

SELECT * FROM deduplicated
WHERE row_num = 1
