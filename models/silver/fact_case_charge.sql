{{ config(
    materialized='incremental',
    unique_key='case_charge_skey',
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

charge_details AS (
    SELECT
        case_pid,
        charge_no,
        charge_type,
        charge_status,
        offence_date,
        _file_date,
        _bronze_loaded_at
    FROM {{ ref('qa_cmplx_criliti_sc_charge_dtls') }} chg
    {% if is_incremental() %}
    CROSS JOIN max_loaded_at
    {% endif %}
    WHERE is_valid_row = TRUE
    {% if is_incremental() %}
        AND chg._bronze_loaded_at > max_loaded_at.cutoff_bronze_loaded_at
    {% endif %}
),

charge_victims AS (
    SELECT
        case_no,
        charge_no,
        entity_name AS victim_name,
        entity_gender AS victim_gender,
        relationship_to_victim,
        offence_group
    FROM {{ ref('qa_info_extracted') }} info
    {% if is_incremental() %}
    CROSS JOIN max_loaded_at
    {% endif %}
    WHERE is_valid_row = TRUE
      AND entity_type = 'Victim'
    {% if is_incremental() %}
        AND info._bronze_loaded_at > max_loaded_at.cutoff_bronze_loaded_at
    {% endif %}
),

case_status_base AS (
    SELECT
        case_pid,
        UPPER(TRIM(case_status)) AS case_status
    FROM {{ ref('qa_ext_criliti_sc') }}
    WHERE is_valid_row = TRUE
),

fact_case_charge_source AS (
    SELECT
        ch.case_pid,
        ch.charge_no,
        ch.charge_type AS offence_type,
        COALESCE(cv.offence_group, 'UNKNOWN') AS offence_group,
        UPPER(TRIM(ch.charge_status)) AS charge_status,
        cs.case_status,
        d_commit.date_skey AS committed_date_skey,
        cv.relationship_to_victim AS relation_to_accused,
        cases.case_skey,
        victims.person_skey AS victim_skey,
        ch._file_date,
        ch._bronze_loaded_at
    FROM charge_details ch
    LEFT JOIN {{ ref('dim_case') }} cases
        ON ch.case_pid = cases.case_pid
    LEFT JOIN charge_victims cv
        ON cases.case_no = cv.case_no
        AND ch.charge_no = cv.charge_no
    LEFT JOIN {{ ref('dim_person') }} victims
        ON cv.victim_name = victims.full_name
        AND cv.victim_gender = victims.gender
    LEFT JOIN {{ ref('dim_date') }} d_commit
        ON ch.offence_date = d_commit.full_date
    LEFT JOIN case_status_base cs
        ON ch.case_pid = cs.case_pid
),

charge_status_reconciled AS (
    SELECT
        *,
        CASE
            WHEN case_status = 'DISP' AND charge_status = 'PENDING' THEN 'SENTENCED'
            ELSE charge_status
        END AS charge_status_reconciled
    FROM fact_case_charge_source
),

case_status_reconciled AS (
    SELECT
        case_pid,
        CASE
            WHEN MAX(CASE WHEN case_status = 'PEND' THEN 1 ELSE 0 END) = 1
                 AND MAX(CASE WHEN charge_status_reconciled = 'PENDING' THEN 1 ELSE 0 END) = 0
                THEN 'DISP'
            ELSE MAX(case_status)
        END AS case_status_reconciled
    FROM charge_status_reconciled
    GROUP BY case_pid
),

fact_case_charge_reconciled AS (
    SELECT
        r_charge.*,
        r_case.case_status_reconciled AS case_status
    FROM charge_status_reconciled r_charge
    LEFT JOIN case_status_reconciled r_case
        ON r_charge.case_pid = r_case.case_pid
),

fact_case_charge_deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY case_pid, charge_no
                ORDER BY _file_date DESC
            ) AS rn
        FROM fact_case_charge_reconciled
    )
    WHERE rn = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['case_pid', 'charge_no']) }} AS case_charge_skey,
    case_skey,
    victim_skey,
    relation_to_accused,
    case_status_reconciled AS case_status,
    charge_status_reconciled AS charge_status,
    committed_date_skey,
    offence_type,
    offence_group,
    _file_date,
    _bronze_loaded_at,
    current_timestamp() AS _silver_loaded_at
FROM fact_case_charge_deduplicated
