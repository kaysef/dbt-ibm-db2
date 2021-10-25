{% macro ibm_db2__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target.quote(schema=False, identifier=False) }} as DBT_INTERNAL_DEST
    using {{ source.quote(schema=False, identifier=False) }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values (
            {%- for cols_csv in insert_cols -%}
                DBT_INTERNAL_SOURCE.{{ cols_csv }}{%- if not loop.last %}, {% endif -%}
            {%- endfor -%}
        )
{% endmacro %}