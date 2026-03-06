{% macro generate_bronze_table(source_name) %}

    {# Look up natural key and column metadata from seeds #}
    {% if execute %}
        {% set table_query %}
            select natural_key
            from {{ ref('metadata_tables') }}
            where source_name = '{{ source_name }}'
              and active = true
        {% endset %}
        {% set table_result = run_query(table_query) %}
        {% set natural_key = table_result.columns['natural_key'].values()[0] %}
        {% set key_columns = natural_key.split(',') | map('trim') | list %}

        {% set col_query %}
            select source_column, target_column, data_type, nullable
            from {{ ref('metadata_columns') }}
            where source_name = '{{ source_name }}'
              and active = true
              and source_column not in ('_source_file', '_ingested_at', '_file_date')
        {% endset %}
        {% set col_rows = run_query(col_query).rows %}
    {% else %}
        {% set key_columns = [] %}
        {% set col_rows = [] %}
    {% endif %}

    with source as (
        select *
        from {{ source('landing', source_name) }}

        {% if is_incremental() %}
        where _ingested_at > (select max(_ingested_at) from {{ this }})
        {% endif %}
    ),

    deduplicated as (
        select *,
            row_number() over (
                partition by {{ key_columns | join(', ') }},
                _file_date
                order by _ingested_at desc
            ) as _row_num
        from source
    ),

    validated as (
        select
            * exclude (_row_num),
            case
                {% for key in key_columns %}
                when {{ key }} is null then 'missing_{{ key | trim }}'
                {% endfor %}
                {% for row in col_rows %}
                {% if row['nullable'] | string | lower == 'false' %}
                when {{ row['source_column'] }} is null then 'missing_{{ row['source_column'] }}'
                {% endif %}
                {% endfor %}
                else null
            end as _rejected_reason
        from deduplicated
        where _row_num = 1
    ),

    casted as (
        select
            {%- for row in col_rows %}
            cast({{ row['source_column'] }} as {{ row['data_type'] }}) as {{ row['target_column'] }},
            {%- endfor %}
            _source_file,
            _file_date,
            _rejected_reason,
            _ingested_at,
            current_timestamp() as _bronze_loaded_at
        from validated
    )

    select * from casted

{% endmacro %}
