
{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_relation = make_temp_relation(target_relation) %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {% do adapter.drop_relation(tmp_relation) %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ log(select) }}
        {{ create_table_as(True, tmp_relation, select) }}
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}


{% macro ibm_db2__post_snapshot(staging_relation) %}
  -- Clean up the snapshot temp table
  {% do drop_relation(staging_relation) %}
{% endmacro %}

{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select tgt_relation.*,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation.quote(schema=False, identifier=False) }} AS tgt_relation
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            sn_query.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query AS sn_query
    ),

    updates_source_data as (

        select
            sn_qun.*,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            {{ strategy.updated_at }} as dbt_valid_to

        from snapshot_query AS sn_qun
    ),

    {%- if strategy.invalidate_hard_deletes %}

    deletes_source_data as (

        select 
            sn_quny.*,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query AS sn_quny
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (
    
        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_updated_at,
            {{ snapshot_get_time() }} as dbt_valid_to,
            snapshotted_data.dbt_scd_id
    
        from snapshotted_data
        left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where source_data.dbt_unique_key is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}

{%- endmacro %}


{% macro build_snapshot_table(strategy, sql) %}

    select sbq.*,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) AS sbq

{% endmacro %}


{% macro snapshot_check_all_get_existing_columns(node, target_exists) -%}
    {%- set query_columns = get_columns_in_query(node['compiled_sql']) -%}
    {%- if not target_exists -%}
        {# no table yet -> return whatever the query does #}
        {{ return([false, query_columns]) }}
    {%- endif -%}
    {# handle any schema changes #}
    {%- set target_table = node.get('alias', node.get('name')) -%}
    {%- set target_relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=target_table) -%}
    {%- set existing_cols = get_columns_in_query('select * from ' ~ target_relation.quote(schema=False, identifier=False)) -%}
    {%- set ns = namespace() -%} {# handle for-loop scoping with a namespace #}
    {%- set ns.column_added = false -%}

    {%- set intersection = [] -%}
    {%- for col in query_columns -%}
        {%- if col in existing_cols -%}
            {%- do intersection.append(col) -%}
        {%- else -%}
            {% set ns.column_added = true %}
        {%- endif -%}
    {%- endfor -%}
    {{ return([ns.column_added, intersection]) }}
{%- endmacro %}


{% materialization snapshot, adapter='ibm_db2' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
    {{ log('checking schema') }}
  {% if not check_schema_exists(model.database, model.schema) %}
    {% do ibm_db2__create_schema(model.database, model.schema) %}
  {% endif %}

    {{ log('setting target relation exists') }}
  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}


  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}
        {{ log('building snapshot table') }}
      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
        {{ log('create table as final sql') }}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% set final_sql = snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {% if not target_relation_exists %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}