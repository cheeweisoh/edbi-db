{% snapshot snap_assigned_lo %}

{{
    config(
        target_schema='snapshots',
        unique_key='case_pid',
        strategy='check',
        check_cols=['officer_id'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    case_pid,
    officer_id,
    _file_date,
    _bronze_loaded_at
FROM {{ ref('qa_cmplx_criliti_sc_assigned_lo') }}
WHERE is_valid_row = TRUE

{% endsnapshot %}
