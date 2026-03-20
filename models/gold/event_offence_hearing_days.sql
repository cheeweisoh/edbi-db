{{ config(
    materialized='table',
    tags=['gold']
) }}

WITH event_officer_base AS (
    SELECT
        feo.court_event_id,
        feo.case_skey,
        do.officer_id,
        feo.court_event_type,
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
        case_type
    FROM {{ ref('fact_case_officer') }}
)

SELECT
    eob.court_event_id,
    cog.offence_group,
    eob.officer_id,
    eob.court_event_type,
    ct.case_type,
    cog.case_status,
    eob.hearing_days
FROM event_officer_base eob
LEFT JOIN case_offence_groups cog
    ON eob.case_skey = cog.case_skey
LEFT JOIN case_types ct
    ON eob.case_skey = ct.case_skey