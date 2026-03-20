{{ config(
    materialized='incremental',
    partition_by=['officer_cluster', 'officer_team'],
    unique_key=['officer_skey', 'row_start_date'],
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
officers AS (
    SELECT DISTINCT
        officer_id,
        officer_name AS full_name,
        cluster AS officer_cluster,
        team AS officer_team,
        CAST(current_date() AS DATE) AS ingestion_date,
        _file_date,
        _bronze_loaded_at
    FROM {{ ref('qa_cmplx_criliti_sc_assigned_lo') }}
    {% if is_incremental() %}
    CROSS JOIN max_loaded_at
    {% endif %}
    WHERE is_valid_row = TRUE
    {% if is_incremental() %}
        AND _bronze_loaded_at > max_loaded_at.cutoff_bronze_loaded_at
    {% endif %}
),

merged_history AS (
    SELECT
        officers.officer_id,
        officers.full_name,
        officers.officer_cluster,
        officers.officer_team,
        CASE
            WHEN existing.officer_skey IS NULL
                THEN officers.ingestion_date
            WHEN existing.full_name != officers.full_name
                OR existing.officer_cluster != officers.officer_cluster
                OR existing.officer_team != officers.officer_team
                THEN officers.ingestion_date
            ELSE existing.row_start_date
        END AS row_start_date,
        CASE
            WHEN existing.officer_skey IS NULL
                AND (
                    existing.full_name != officers.full_name
                    OR existing.officer_cluster != officers.officer_cluster
                    OR existing.officer_team != officers.officer_team
                )
                THEN DATE_SUB(officers.ingestion_date, 1)
            ELSE NULL
        END AS row_end_date,
        officers._file_date,
        officers._bronze_loaded_at
    FROM officers
    LEFT JOIN {{ this }} AS existing
        ON officers.officer_id = existing.officer_id
        AND existing.row_end_date IS NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['officer_id', 'row_start_date']) }} AS officer_skey,
    officer_id,
    full_name,
    officer_cluster,
    officer_team,
    row_start_date,
    row_end_date,
    _file_date,
    _bronze_loaded_at,
    current_timestamp() AS _silver_loaded_at
FROM merged_history
