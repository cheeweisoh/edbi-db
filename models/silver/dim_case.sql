{{ config(
    materialized='incremental',
    unique_key='case_skey',
    incremental_strategy='merge',
    tags=['silver']
) }}

WITH cases AS (
    SELECT DISTINCT
        case_pid,
        accused_name,
        accused_gender,
        TRY_CAST(accused_dob AS DATE) AS accused_dob,
        case_no,
        case_title,
        directorate,
    FROM {{ ref('qa_ext_criliti_sc') }}
    WHERE is_valid_row = TRUE
),

cases_with_accused AS (
    SELECT
        cases.*
        persons.person_skey
    FROM cases
    LEFT JOIN {{ ref('dim_person') }} AS persons
        ON cases.accused_name = persons.full_name
        AND cases.accused_gender = persons.gender
        AND cases.accused_dob = persons.date_of_birth
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['case_no', 'case_pid']) }} AS case_skey,
    person_skey AS accused_skey,
    case_no,
    case_pid,
    directorate,
FROM cases_with_accused
