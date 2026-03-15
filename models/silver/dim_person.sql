{{ config(
    materialized='incremental',
    unique_key='person_skey',
    incremental_strategy='merge',
    tags=['silver']
) }}

WITH case_accused AS (
    SELECT DISTINCT
        accused_name AS full_name,
        accused_gender AS gender,
        TRY_CAST(accused_dob AS DATE) AS date_of_birth
    FROM {{ ref('qa_ext_criliti_sc') }}
    WHERE is_valid_row = TRUE
),

extracted_persons AS (
    SELECT DISTINCT
        entity_name AS full_name,
        entity_gender AS gender,
        NULL AS date_of_birth
    FROM {{ ref('qa_info_extracted') }}
    WHERE is_valid_row = TRUE
),

persons AS (
    SELECT * FROM case_accused
    UNION
    SELECT * FROM extracted_persons
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['full_name', 'gender', 'date_of_birth']) }} AS person_skey,
    full_name,
    gender,
    date_of_birth
FROM persons
