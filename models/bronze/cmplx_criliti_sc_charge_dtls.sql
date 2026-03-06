-- depends_on: {{ ref('metadata_tables') }}
-- depends_on: {{ ref('metadata_columns') }}

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    partition_by='_file_date',
    on_schema_change='fail'
) }}

{{ generate_bronze_table('cmplx_criliti_sc_charge_dtls') }}
