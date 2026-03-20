{{ config(
    materialized='table',
    tags=['gold']
) }}

WITH event_officer_base AS (
    SELECT
        feo.court_event_id,
        feo.case_skey,
        feo.officer_skey,
        do.officer_id,
        feo.court_event_type,
        feo.court_event_date_skey,
        feo.court_event_hearing_days AS hearing_days
    FROM {{ ref('fact_event_officer') }} feo
    LEFT JOIN {{ ref('dim_officer') }} do
        ON feo.officer_skey = do.officer_skey
),

case_offence_groups AS (
    SELECT DISTINCT
        case_skey,
        offence_group,
        case_status
    FROM {{ ref('fact_case_charge') }}
),

case_types AS (
    SELECT DISTINCT
        case_skey,
        case_type,
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

last_event_date_dim AS (
    SELECT
        date_skey,
        full_date AS last_court_event_date
    FROM {{ ref('dim_date') }}
)

SELECT
    eob.court_event_id,
    cog.offence_group,
    eob.officer_id,
    eob.court_event_type,
    ct.case_type,
    cog.case_status,
    oc.officer_cluster,
    fmd.first_mention_date,
    lemd.last_court_event_date,
    eob.hearing_days
FROM event_officer_base eob
LEFT JOIN case_offence_groups cog
    ON eob.case_skey = cog.case_skey
LEFT JOIN case_types ct
    ON eob.case_skey = ct.case_skey
LEFT JOIN officer_cluster oc
    ON eob.officer_skey = oc.officer_skey
LEFT JOIN first_mention_date_dim fmd
    ON ct.first_mention_date_skey = fmd.date_skey
LEFT JOIN last_event_date_dim lemd
    ON eob.court_event_date_skey = lemd.date_skey