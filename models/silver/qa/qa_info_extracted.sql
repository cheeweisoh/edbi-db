{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['case_no', 'charge_no', 'entity_name', '_file_date'],
    partition_by=['_file_date'],
    on_schema_change='fail',
    tags=['silver', 'qa']
)}}

WITH
{% if is_incremental() %}
max_loaded_at AS (
    SELECT COALESCE(MAX(_bronze_loaded_at), TIMESTAMP('1900-01-01')) AS cutoff_bronze_loaded_at
    FROM {{ this }}
),
{% endif %}

base AS (
    SELECT
        case_no,
        charge_no, 
        entity_name,
        entity_type,
        entity_age,
        entity_gender,
        relationship_to_victim,
        offence_group,
        special_type,
        _rescued_data,
        _source_file,
        _file_date,
        _ingested_at,
        _bronze_loaded_at,

        -- quality flags
        CASE WHEN case_no IS NULL THEN false ELSE true END AS _dq_missing_case_no,
        CASE WHEN charge_no IS NULL THEN false ELSE true END AS _dq_missing_charge_no,
        CASE WHEN entity_name IS NULL THEN false ELSE true END AS _dq_missing_entity_name,
        CASE WHEN entity_type IS NULL THEN false ELSE true END AS _dq_missing_entity_type,
        CASE WHEN entity_type NOT IN ('Accused Person', 'Victim') THEN false ELSE true END AS _dq_invalid_entity_type,
        CASE WHEN entity_age NOT BETWEEN 0 AND 120 THEN false ELSE true END AS _dq_invalid_entity_age,
        CASE WHEN entity_gender NOT IN ('M', 'F') THEN false ELSE true END AS _dq_invalid_entity_gender,
        CASE WHEN offence_group IS NULL THEN false ELSE true END AS _dq_missing_offence_group
    FROM {{ ref('info_extracted') }} src
    {% if is_incremental() %}
    CROSS JOIN max_loaded_at
    {% endif %}
    WHERE _rejected_reason IS NULL
    {% if is_incremental() %}
        AND src._bronze_loaded_at > max_loaded_at.cutoff_bronze_loaded_at
    {% endif %}
)

SELECT
    *,
    CASE
        WHEN _dq_missing_case_no = true THEN true
        WHEN _dq_missing_charge_no = true THEN true
        WHEN _dq_missing_entity_name = true THEN true
        WHEN _dq_missing_entity_type = true THEN true
        WHEN _dq_invalid_entity_type = true THEN true
        WHEN _dq_invalid_entity_age = true THEN true
        WHEN _dq_invalid_entity_gender = true THEN true
        WHEN _dq_missing_offence_group = true THEN true
        ELSE false
    END AS is_valid_row
FROM base       
