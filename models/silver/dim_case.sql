{{ config(
    materialized='incremental',
    unique_key='case_skey',
    incremental_strategy='merge',
    tags=['silver']
) }}

WITH
{% if is_incremental() %}
max_loaded_at AS (
    SELECT COALESCE(MAX(_bronze_loaded_at), TIMESTAMP('1900-01-01')) AS cutoff_bronze_loaded_at
    FROM {{ this }}
),
{% endif %}

cases AS (
    SELECT DISTINCT
        case_pid,
        accused_name,
        accused_gender,
        TRY_CAST(accused_dob AS DATE) AS accused_dob,
        case_no,
        case_title,
        directorate,
        _file_date,
        _bronze_loaded_at
    FROM {{ ref('qa_ext_criliti_sc') }}
    {% if is_incremental() %}
    CROSS JOIN max_loaded_at
    {% endif %}
    WHERE is_valid_row = TRUE
    {% if is_incremental() %}
        AND _bronze_loaded_at > max_loaded_at.cutoff_bronze_loaded_at
    {% endif %}
),

cases_with_accused AS (
    SELECT DISTINCT
        cases.*,
        persons.person_skey
    FROM cases
    LEFT JOIN {{ ref('dim_person') }} AS persons
        ON COALESCE(cases.accused_name, '') = COALESCE(persons.full_name, '')
        AND COALESCE(cases.accused_gender, '') = COALESCE(persons.gender, '')
        AND COALESCE(cases.accused_dob, '1900-01-01') = COALESCE(persons.date_of_birth, '1900-01-01')
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['case_no', 'case_pid']) }} AS case_skey,
    person_skey AS accused_skey,
    case_no,
    case_pid,
    directorate,
    _file_date,
    _bronze_loaded_at,
    current_timestamp() AS _silver_loaded_at
FROM cases_with_accused
