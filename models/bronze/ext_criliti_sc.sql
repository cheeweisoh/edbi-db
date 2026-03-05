{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partition_by='_file_date'
) }}

SELECT *
FROM {{ source('landing', 'ext_criliti_sc') }}

{% if is_incremental() %}
WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
{% endif %}
