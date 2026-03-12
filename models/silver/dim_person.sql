{{ config(
    materialized='incremental',
    unique_key='person_skey',
    incremental_strategy='merge',
    tags=['silver']
) }}

WITH persons AS (
    SELECT DISTINCT
        accused_name AS full_name,
        accused_gender AS gender,
        TRY_CAST(accused_dob AS DATE) AS date_of_birth
    FROM {{ ref('qa_ext_criliti_sc') }}
    WHERE is_valid_row = TRUE
)

SELECT
    MD5(CONCAT_WS('|',
        COALESCE(full_name, ''),
        COALESCE(CAST(date_of_birth AS STRING), ''),
        COALESCE(gender, '')
    )) AS person_skey,
    full_name,
    gender,
    date_of_birth
FROM persons
