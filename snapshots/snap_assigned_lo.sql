{% snapshot snap_assigned_lo %}

{{
    config(
        target_schema='snapshots',
        unique_key=['case_pid', 'officer_id'],
        strategy='check',
        check_cols=['officer_name', 'cluster', 'team', '_file_date'],
        invalidate_hard_deletes=True
    )
}}

WITH ranked AS (
   SELECT
        *,
        row_number() OVER (PARTITION BY case_pid ORDER BY _file_date DESC) rn
   FROM {{ ref('qa_cmplx_criliti_sc_assigned_lo') }}
   WHERE is_valid_row = true
 )

 SELECT
    case_pid,
    officer_id,
    officer_name,
    cluster,
    team,
    _file_date,
    _bronze_loaded_at
 FROM ranked
 WHERE rn = 1

{% endsnapshot %}
