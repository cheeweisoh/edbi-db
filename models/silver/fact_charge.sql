{{ config(
    materialized='incremental',
    unique_key='charge_skey',
    incremental_strategy='merge',
    tags=['silver']
) }}

WITH charge_details AS (
    SELECT
        case_pid,
        charge_no,
        charge_type,
        charge_status,
        offence_date,
        offence_group,
        _file_date,
        _bronze_loaded_at
    FROM {{ ref('qa_cmplx_criliti_sc_charge_dtls') }}
    WHERE is_valid_row = TRUE
),

charge_victims AS (
    SELECT
        case_no,
        charge_no,
        entity_name AS victim_name,
        entity_gender AS victim_gender,
        relationship_to_victim,
        offence_group
    FROM {{ ref('qa_info_extracted') }}
    WHERE is_valid_row = TRUE
      AND entity_type = 'Victim'
),

case_keys AS (
    SELECT
        case_skey,
        case_pid,
        case_no
    FROM {{ ref('dim_case') }}
),

victim_keys AS (
    SELECT
        person_skey AS victim_skey,
        full_name,
        gender
    FROM {{ ref('dim_person') }}
),

committed_date AS (
    SELECT
        date_skey,
        full_date
    FROM {{ ref('dim_date') }}
),

fact_case_charge_source AS (
    SELECT
        ch.case_pid,
        ch.charge_no,
        ch.charge_type AS offence_type,
        COALESCE(ch.offence_group, v.offence_group, 'UNKNOWN') AS offence_group,
        ch.charge_status,
        ch.offence_date,
        ch._file_date,
        v.relationship_to_victim AS relation_to_accused,
        vc.case_skey,
        vp.victim_skey
    FROM charge_details ch
    LEFT JOIN case_keys vc
        ON ch.case_pid = vc.case_pid
    LEFT JOIN charge_victims v
        ON vc.case_no = v.case_no
       AND ch.charge_no = v.charge_no
    LEFT JOIN victim_keys vp
        ON v.victim_name = vp.full_name
       AND v.victim_gender = vp.gender
),

fact_case_charge_prepped AS (
    SELECT
        *,
        d.date_skey AS committed_date_skey
    FROM fact_case_charge_source f
    LEFT JOIN committed_date d
        ON f.offence_date = d.full_date
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['case_pid', 'charge_no']) }} AS charge_skey,
    case_skey,
    victim_skey,
    relation_to_accused,
    charge_status,
    committed_date_skey,
    offence_type,
    offence_group,
    _file_date,
    _bronze_loaded_at
FROM fact_case_charge_prepped

{% if is_incremental() %}
    WHERE _file_date > (SELECT MAX(_file_date) FROM {{ this }})
{% endif %}
