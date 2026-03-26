{{ config(
    materialized='table',
    tags=['gold']
) }}

WITH base AS (
    SELECT
        fco.case_skey,
        fco.officer_skey,
        fco.assigned_from_date_skey,
        fco.assigned_to_date_skey,
        fco.first_mention_date_skey,
        fco.case_status,
        fco.case_complexity,
        fco.first_mention_year
    FROM {{ ref('fact_case_officer') }} AS fco
),

officer_meta AS (
    SELECT
        officer_skey,
        officer_id AS assigned_officer_id,
        full_name AS assigned_officer_name,
        officer_cluster,
        officer_team
    FROM {{ ref('dim_officer') }}
),

case_meta AS (
    SELECT
        case_skey,
        case_no,
        case_title,
        directorate
    FROM {{ ref('dim_case') }}
),

first_mention_date AS (
    SELECT
        date_skey,
        full_date AS first_mention_date
    FROM {{ ref('dim_date') }}
),

case_disposition AS (
    SELECT
        feo.case_skey,
        CASE
            WHEN b.case_status = 'DISP' THEN MAX(d.full_date)
            ELSE NULL
        END AS case_disposition_date
    FROM {{ ref('fact_event_officer') }} feo
    LEFT JOIN base b
        ON feo.case_skey = b.case_skey
    LEFT JOIN {{ ref('dim_date') }} d
        ON feo.court_event_date_skey = d.date_skey
    GROUP BY
        feo.case_skey,
        b.case_status
)

SELECT
    om.assigned_officer_name,
    om.officer_cluster,
    b.case_status,
    b.case_complexity,
    fmd.first_mention_date,
    COUNT(1) AS case_count,
    SUM(CASE WHEN b.assigned_to_date_skey IS NULL THEN 1 ELSE 0 END) AS currently_assigned_count,
    cd.case_disposition_date,
    DATEDIFF(COALESCE(cd.case_disposition_date, CURRENT_DATE()), fmd.first_mention_date) AS case_processing_days
FROM base b
LEFT JOIN officer_meta om ON b.officer_skey = om.officer_skey
LEFT JOIN case_meta cm ON b.case_skey = cm.case_skey
LEFT JOIN first_mention_date fmd ON b.first_mention_date_skey = fmd.date_skey
LEFT JOIN case_disposition cd ON b.case_skey = cd.case_skey
GROUP BY
    om.assigned_officer_name,
    om.officer_cluster,
    b.case_status,
    b.case_complexity,
    fmd.first_mention_date,
    cd.case_disposition_date
