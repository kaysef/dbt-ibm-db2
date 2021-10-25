{% macro ibm_db2__validate_get_incremental_strategy(config) %}
    {# -- Find and validate the incremental strategy  #}
    {% set strategy = config.get("incremental_strategy", default="merge") %}

    {% set invalid_strategy_msg -%}
        Invalid incremental strategy provided: {{ strategy }}
        Expected one of: 'merge', 'delete_insert', 'update_insert'
    {%- endset %}
    {% if strategy not in ['merge', 'delete_insert', 'update_insert'] %}
        {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
    {% endif %}

    {% do return(strategy) %}
{% endmacro %}




{% macro ibm_db2__get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) %}
    {% if strategy == 'merge' %}
        {% do return(ibm_db2__incremental_upsert(tmp_relation, target_relation, unique_key)) %}
    {% elif strategy == 'delete_insert' %}
        {% do return(ibm_db2__incremental_delete_insert(tmp_relation, target_relation, unique_key)) %}
    {% elif strategy == 'update_insert' %}
        {% do return(ibm_db2__incremental_update_insert(tmp_relation, target_relation, unique_key)) %}
    {% else %}
        {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
    {% endif %}
{% endmacro %}




{% materialization incremental, adapter='ibm_db2' -%}

  {% set unique_key = config.get('unique_key') %}
  {% set full_refresh_mode = flags.FULL_REFRESH %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}
  {% set strategy = ibm_db2__validate_get_incremental_strategy(config) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% do adapter.drop_relation(tmp_relation) %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do to_drop.append(tmp_relation) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {% set build_sql = ibm_db2__get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) %}
  {% endif %}

  {% call statement("main") %}
    {{ build_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {# rasmus: moved here before commit #}
  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}