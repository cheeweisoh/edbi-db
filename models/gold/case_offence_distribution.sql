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
        case_type,
        officer_skey,
        first_mention_date_skey
    FROM {{ ref('fact_case_officer') }}
),

officer_cluster AS (
    SELECT
        officer_skey,
        officer_cluster
    FROM {{ ref('dim_officer') }}
),

first_mention_date_dim AS (
    SELECT
        date_skey,
        full_date AS first_mention_date
    FROM {{ ref('dim_date') }}
),

last_event_date AS (
    SELECT
        case_skey,
        MAX(d.full_date) AS last_court_event_date
    FROM {{ ref('fact_event_officer') }} feo
    LEFT JOIN {{ ref('dim_date') }} d
        ON feo.court_event_date_skey = d.date_skey
    GROUP BY case_skey
)

SELECT
    cm.case_no,
    cob.offence_group,
    ca.case_status,
    ca.case_type,
    oc.officer_cluster,
    fmd.first_mention_date,
    led.last_court_event_date,
    COUNT(1) AS case_count
FROM case_offence_base cob
LEFT JOIN case_meta cm
    ON cob.case_skey = cm.case_skey
LEFT JOIN case_attributes ca
    ON cob.case_skey = ca.case_skey
LEFT JOIN officer_cluster oc
    ON ca.officer_skey = oc.officer_skey
LEFT JOIN first_mention_date_dim fmd
    ON ca.first_mention_date_skey = fmd.date_skey
LEFT JOIN last_event_date led
    ON cob.case_skey = led.case_skey
GROUP BY
    cm.case_no,
    cob.offence_group,
    ca.case_status,
    ca.case_type,
    oc.officer_cluster,
    fmd.first_mention_date,
    led.last_court_event_date
