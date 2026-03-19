{{ config(
    materialized='incremental',
    unique_key=['case_skey', 'officer_skey', 'assigned_from_date_skey'],
    incremental_strategy='merge',
    tags=['silver']
) }}

WITH assigned_base AS (
    SELECT
        case_pid,
        officer_id,
        _file_date,
        _bronze_loaded_at
    FROM {{ ref('qa_cmplx_criliti_sc_assigned_lo') }}
    WHERE is_valid_row = TRUE
    {% if is_incremental() %}
        AND _bronze_loaded_at > (SELECT MAX(_bronze_loaded_at) FROM {{ this }})
    {% endif %}
),

assignment_with_leads AS (
    SELECT
        case_pid,
        officer_id,
        _file_date,
        LAG(officer_id) OVER (PARTITION BY case_pid ORDER BY _file_date) AS prev_officer
    FROM assigned_base
),

assignment_grouped AS (
    SELECT
        case_pid,
        officer_id,
        _file_date,
        SUM(CASE WHEN prev_officer IS NULL OR prev_officer != officer_id THEN 1 ELSE 0 END) OVER (PARTITION BY case_pid ORDER BY _file_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS assignment_group
    FROM assignment_with_leads
),

assignment_periods AS (
    SELECT
        case_pid,
        officer_id,
        MIN(_file_date) AS assigned_from_date,
        MAX(_file_date) AS period_last_date
    FROM assignment_grouped
    GROUP BY case_pid, officer_id, assignment_group
),

assignment_periods_with_to AS (
    SELECT
        case_pid,
        officer_id,
        assigned_from_date,
        period_last_date,
        LEAD(assigned_from_date) OVER (PARTITION BY case_pid ORDER BY assigned_from_date) AS next_assigned_from_date
    FROM assignment_periods
),

assignment_periods_final AS (
    SELECT
        case_pid,
        officer_id,
        assigned_from_date,
        CASE
            WHEN next_assigned_from_date IS NULL THEN NULL
            ELSE DATEADD(day, -1, next_assigned_from_date)
        END AS assigned_to_date
    FROM assignment_periods_with_to
),

case_status AS (
    SELECT
        case_pid,
        upper(trim(case_status)) AS case_status
    FROM {{ ref('qa_ext_criliti_sc') }}
    WHERE is_valid_row = TRUE
),

case_type_flags AS (
    SELECT
        case_pid,
        CASE
            WHEN MAX(CASE WHEN court_event_type IN ('TRIAL', 'PH') THEN 1 ELSE 0 END) = 1 THEN 'Trial'
            ELSE 'PG'
        END AS case_type
    FROM {{ ref('qa_tb_criliti_sc_court_events') }}
    WHERE is_valid_row = TRUE
    GROUP BY case_pid
),

first_mention AS (
    SELECT
        case_pid,
        MIN(DATE_TRUNC('day', start_datetime)) AS first_mention_date
    FROM {{ ref('qa_tb_criliti_sc_court_events') }}
    WHERE is_valid_row = TRUE
      AND court_event_type = 'FM'
    GROUP BY case_pid
),

date_dim AS (
    SELECT
        date_skey,
        full_date
    FROM {{ ref('dim_date') }}
),

case_dim AS (
    SELECT
        case_skey,
        case_pid
    FROM {{ ref('dim_case') }}
),

officer_dim AS (
    SELECT
        officer_skey,
        officer_id
    FROM {{ ref('dim_officer') }}
),

fact_case_officer_source AS (
    SELECT
        a.case_pid,
        a.officer_id,
        c.case_skey,
        o.officer_skey,
        d_assigned.date_skey AS assigned_from_date_skey,
        d_assigned_to.date_skey AS assigned_to_date_skey,
        d_first_mention.date_skey AS first_mention_date_skey,
        cs.case_status,
        ct.case_type,
        EXTRACT(YEAR FROM first_mention.first_mention_date) AS first_mention_year,
        a.assigned_from_date AS _file_date,
        NULL AS _bronze_loaded_at
    FROM assignment_periods_final a
    LEFT JOIN case_dim c
        ON a.case_pid = c.case_pid
    LEFT JOIN officer_dim o
        ON a.officer_id = o.officer_id
    LEFT JOIN first_mention
        ON a.case_pid = first_mention.case_pid
    LEFT JOIN date_dim d_assigned
        ON TRY_CAST(a.assigned_from_date AS DATE) = d_assigned.full_date
    LEFT JOIN date_dim d_assigned_to
        ON a.assigned_to_date IS NOT NULL
        AND TRY_CAST(a.assigned_to_date AS DATE) = d_assigned_to.full_date
    LEFT JOIN date_dim d_first_mention
        ON first_mention.first_mention_date = d_first_mention.full_date
    LEFT JOIN case_status cs
        ON a.case_pid = cs.case_pid
    LEFT JOIN case_type_flags ct
        ON a.case_pid = ct.case_pid
)

SELECT
    case_skey,
    officer_skey,
    assigned_from_date_skey,
    assigned_to_date_skey,
    first_mention_date_skey,
    case_status,
    case_type,
    first_mention_year,
    _file_date,
    _bronze_loaded_at
FROM fact_case_officer_source
