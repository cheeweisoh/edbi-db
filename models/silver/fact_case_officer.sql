{{ config(
    materialized='incremental',
    unique_key=['case_officer_skey'],
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

assigned_base AS (
    SELECT
        case_pid,
        officer_id,
        CAST(dbt_valid_from AS DATE) AS assigned_from_date,
        CAST(dbt_valid_to AS DATE) AS assigned_to_date,
        dbt_updated_at,
        _file_date,
        _bronze_loaded_at
    FROM {{ ref('snap_assigned_lo') }} snap
    {% if is_incremental() %}
    CROSS JOIN max_loaded_at
    {% endif %}
    WHERE is_valid_row = TRUE
    {% if is_incremental() %}
        AND snap._bronze_loaded_at > max_loaded_at.cutoff_bronze_loaded_at
    {% endif %}
),

case_status AS (
    SELECT
        case_pid,
        upper(trim(case_status)) AS case_status
    FROM {{ ref('qa_ext_criliti_sc') }}
    WHERE is_valid_row = TRUE
),

case_flags AS (
    SELECT
        case_pid,
        CASE
            WHEN MAX(CASE WHEN court_event_type IN ('TRIAL', 'PH') THEN 1 ELSE 0 END) = 1 THEN 'Trial'
            ELSE 'PG'
        END AS case_type,
        MAX(CASE WHEN court_event_type IN ('TRIAL', 'PH') THEN 1 ELSE 0 END) AS trial_ph_flag
    FROM {{ ref('qa_tb_criliti_sc_court_events') }}
    WHERE is_valid_row = TRUE
        AND court_event_status = 'NEW'
    GROUP BY case_pid
),

first_mention AS (
    SELECT
        case_pid,
        MIN(DATE_TRUNC('day', start_datetime)) AS first_mention_date
    FROM {{ ref('qa_tb_criliti_sc_court_events') }}
    WHERE is_valid_row = TRUE
    GROUP BY case_pid
),

charge_count AS (
    SELECT
        case_skey,
        COUNT(*) AS charge_count
    FROM {{ ref('fact_case_charge') }}
    GROUP BY case_skey
),

special_flag AS (
    SELECT
        case_no,
        1 AS has_special_type
    FROM {{ ref('qa_info_extracted') }}
    WHERE is_valid_row = TRUE
      AND special_type IS NOT NULL
    GROUP BY case_no
),

fact_case_officer_source AS (
    SELECT
        a.case_pid,
        a.officer_id,
        c.case_skey,
        a.officer_id,
        o.officer_skey,
        a.assigned_from_date,
        a.assigned_to_date,
        d_assigned.date_skey AS assigned_from_date_skey,
        d_assigned_to.date_skey AS assigned_to_date_skey,
        d_first_mention.date_skey AS first_mention_date_skey,
        cs.case_status,
        cf.case_type,
        CASE
            WHEN COALESCE(sf.has_special_type, 0) = 1 THEN 'Complex'
            WHEN COALESCE(cf.trial_ph_flag, 0) = 1 THEN 'Complex'
            WHEN COALESCE(cc.charge_count, 0) >= 3 THEN 'Complex'
            ELSE 'Simple'
        END AS case_complexity,
        EXTRACT(YEAR FROM fm.first_mention_date) AS first_mention_year,
        a.dbt_updated_at AS _officer_snapshot_date,
        a._file_date,
        a._bronze_loaded_at
    FROM assigned_base a
    LEFT JOIN {{ ref('dim_case') }} c
        ON a.case_pid = c.case_pid
    LEFT JOIN first_mention fm
        ON a.case_pid = fm.case_pid
    LEFT JOIN case_status cs
        ON a.case_pid = cs.case_pid
    LEFT JOIN case_flags cf
        ON a.case_pid = cf.case_pid
    LEFT JOIN charge_count cc
        ON c.case_skey = cc.case_skey
    LEFT JOIN special_flag sf
        ON c.case_no = sf.case_no
    LEFT JOIN {{ ref('dim_officer') }} o
        ON a.officer_id = o.officer_id
    LEFT JOIN {{ ref('dim_date') }} d_assigned
        ON TRY_CAST(a.assigned_from_date AS DATE) = d_assigned.full_date
    LEFT JOIN {{ ref('dim_date') }} d_assigned_to
        ON a.assigned_to_date IS NOT NULL
        AND TRY_CAST(a.assigned_to_date AS DATE) = d_assigned_to.full_date
    LEFT JOIN {{ ref('dim_date') }} d_first_mention
        ON fm.first_mention_date = d_first_mention.full_date
),

fact_case_officer_deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY case_pid, officer_id, assigned_from_date
                ORDER BY _file_date DESC
            ) AS rn
        FROM fact_case_officer_source
    )
    WHERE rn = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['case_pid', 'officer_id', 'assigned_from_date']) }} AS case_officer_skey,
    case_skey,
    officer_skey,
    assigned_from_date_skey,
    assigned_to_date_skey,
    first_mention_date_skey,
    case_status,
    case_type,
    case_complexity,
    first_mention_year,
    _officer_snapshot_date,
    _file_date,
    _bronze_loaded_at,
    current_timestamp() AS _silver_loaded_at
FROM fact_case_officer_source
