{{ config(
    materialized='table',
    tags=['gold']
) }}

WITH charge_base AS (
    SELECT
        case_skey,
        offence_group,
        COUNT(*) AS charge_count
    FROM {{ ref('fact_charge') }}
    GROUP BY
        case_skey,
        offence_group
),

case_meta AS (
    SELECT
        case_skey,
        case_no
    FROM {{ ref('dim_case') }}
),

case_attributes AS (
    SELECT DISTINCT
        case_skey,
        case_status,
        case_type
    FROM {{ ref('fact_case_officer') }}
)

SELECT
    cm.case_no,
    cb.offence_group,
    ca.case_status,
    ca.case_type,
    cb.charge_count
FROM charge_base cb
LEFT JOIN case_meta cm
    ON cb.case_skey = cm.case_skey
LEFT JOIN case_attributes ca
    ON cb.case_skey = ca.case_skey
