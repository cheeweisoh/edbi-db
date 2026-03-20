{{ config(
    materialized='table',
    tags=['gold']
) }}

WITH case_offence_base AS (
    SELECT
        case_skey,
        offence_group
    FROM {{ ref('fact_case_charge') }}
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
    cob.offence_group,
    ca.case_status,
    ca.case_type,
    COUNT(1) AS case_count
FROM case_offence_base cob
LEFT JOIN case_meta cm
    ON cob.case_skey = cm.case_skey
LEFT JOIN case_attributes ca
    ON cob.case_skey = ca.case_skey
GROUP BY
    cm.case_no,
    cob.offence_group,
    ca.case_status,
    ca.case_type
