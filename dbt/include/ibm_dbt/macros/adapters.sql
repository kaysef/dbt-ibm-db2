
{% macro ibm_dbt__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
        SELECT COUNT(*)
        FROM QSYS2.SCHEMATA
        WHERE UPPER(SCHEMA_NAME) = UPPER('{{ schema }}')
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}


{% macro ibm_dbt__create_schema(relation) -%}
  {%- call statement('create_schema') -%}

  BEGIN
     IF NOT EXISTS (
       SELECT SCHEMA_NAME
       FROM QSYS2.SCHEMATA
       WHERE SCHEMA_NAME = UPPER('{{ relation.schema }}')
     ) THEN
        PREPARE stmt FROM 'CREATE SCHEMA {{ relation.quote(schema=False).schema }}';
        EXECUTE stmt;
     END IF;
  END

  {%- endcall -%}
{% endmacro %}


{% macro ibm_dbt__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}

  BEGIN
  	FOR t AS
      SELECT
        TABLE_NAME,
        TABLE_SCHEMA,
        (CASE WHEN TABLE_TYPE LIKE '%TABLE%' THEN 'TABLE' ELSE 'VIEW' END) AS TABLE_TYPE
      FROM QSYS2.TABLES t
      WHERE TABLE_SCHEMA = UPPER('{{ relation.schema }}')
  		DO
  			PREPARE stmt FROM 'DROP '||t.TABLE_TYPE||' '||t.TABLE_SCHEMA||'.'||t.TABLE_NAME;
  			EXECUTE stmt;
  	END FOR;
    IF EXISTS (
      SELECT SCHEMA_NAME
      FROM QSYS2.SCHEMATA
      WHERE SCHEMA_NAME = UPPER('{{ relation.schema }}')
    ) THEN
      PREPARE stmt FROM 'DROP SCHEMA {{ relation.schema }} RESTRICT';
      EXECUTE stmt;
    END IF;
  END

  {% endcall %}
{% endmacro %}


{% macro ibm_dbt__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {# Ignore temporary table type #}
  CREATE TABLE {{ relation.quote(schema=False, identifier=False) }}
  AS (
    {{ sql }}
  ) WITH DATA

{%- endmacro %}


{% macro ibm_dbt__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  CREATE VIEW {{ relation.quote(schema=False, identifier=False) }} AS
  {{ sql }}

{% endmacro %}


{% macro ibm_dbt__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}

      SELECT
          TRIM(COLUMN_NAME) AS "name",
          TRIM(DATA_TYPE) AS "type",
          CHARACTER_MAXIMUM_LENGTH AS "character_maximum_length",
          NUMERIC_PRECISION AS "numeric_precision",
          NUMERIC_SCALE AS "numeric_scale"
      FROM QSYS2.COLUMNS
      WHERE TABLE_NAME = UPPER('{{ relation.identifier }}')
        {% if relation.schema %}
        AND TABLE_SCHEMA = UPPER('{{ relation.schema }}')
        {% endif %}
      ORDER BY ORDINAL_POSITION

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro ibm_dbt__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}

  SELECT
    TRIM(LOWER(TABLE_CATALOG)) AS "database",
    TRIM(LOWER(TABLE_NAME)) as "name",
    TRIM(LOWER(TABLE_SCHEMA)) as "schema",
    CASE
      WHEN TABLE_TYPE LIKE '%TABLE%' THEN 'table'
      WHEN TABLE_TYPE = 'VIEW' THEN 'view'
    END AS "table_type"
  FROM QSYS2.TABLES
  WHERE
    TABLE_SCHEMA = UPPER('{{ schema_relation.schema }}') 
    AND (TABLE_TYPE = 'VIEW' OR TABLE_TYPE LIKE '%TABLE%')
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro ibm_dbt__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}

    {#
      Not possible to rename views in DB2 so we have to do some work. The DDL
      is selected from syscat.views and a new renamed view is created based on
      this DDL. Comments is removed from the DDL by using regexp but this could
      probably be done better.
    #}
    BEGIN
      DECLARE rename_stmt VARCHAR(1000);
      DECLARE create_stmt VARCHAR(10000);
      DECLARE delete_stmt VARCHAR(1000);

      IF EXISTS (
        SELECT TABLE_NAME
        FROM QSYS2.TABLES
        WHERE TABLE_NAME = UPPER('{{ from_relation.identifier }}') AND TABLE_SCHEMA = UPPER('{{ from_relation.schema }}') AND TABLE_TYPE LIKE '%TABLE%'
      ) THEN
        SET rename_stmt = 'RENAME TABLE {{ from_relation.quote(schema=False, identifier=False) }} TO {{ to_relation.quote(identifier=False).identifier }}';
        PREPARE stmt FROM rename_stmt;
        EXECUTE stmt;
      ELSEIF EXISTS (
        SELECT TABLE_NAME
        FROM QSYS2.TABLES
        WHERE TABLE_NAME = UPPER('{{ from_relation.identifier }}') AND TABLE_SCHEMA = UPPER('{{ from_relation.schema }}') AND TYPE = 'VIEW'
      ) THEN
        SET create_stmt = (
          -- improve regexp here, use regexp_replace instead?
          -- ...or (much better solution if possible) rename view.
          SELECT
            CONCAT(
              'CREATE VIEW {{ to_relation.quote(schema=False, identifier=False) }} AS ',
              -- remove 'create view as'
              REGEXP_REPLACE(
                -- remove comments here (single and multiline)
                REGEXP_REPLACE(
                  text,
                  '(/\*(.|[\r\n])*?\*/)|(--(.*|[\r\n]))','', 1, 1, 'i' -- removing comments
                ),
                '.*CREATE.+VIEW.+AS', '', 1, 1, 'i' -- removing CREATE (OR REPLACE) VIEW AS'
              )
            )
          FROM QSYS2.VIEWS
          WHERE TABLE_SCHEMA = UPPER('{{ from_relation.schema }}') AND TABLE_NAME = UPPER('{{ from_relation.identifier }}')
        );
        PREPARE stmt FROM create_stmt;
        EXECUTE stmt;
        PREPARE stmt FROM 'DROP VIEW {{ from_relation.quote(schema=False, identifier=False) }}';
        EXECUTE stmt;
      END IF;
    END

  {%- endcall %}
{% endmacro %}


{% macro ibm_dbt__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        SELECT 
            DISTINCT TRIM(SCHEMA_NAME) AS "schema"
        FROM QSYS2.SCHEMATA
    {%- endcall %}

    {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro ibm_dbt__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}

    BEGIN
      IF EXISTS (
        SELECT TABLE_NAME
        FROM QSYS2.TABLES
        WHERE TABLE_NAME = UPPER('{{ relation.identifier }}') AND TABLE_SCHEMA = UPPER('{{ relation.schema }}') AND TABLE_TYPE LIKE '%TABLE%'
      ) THEN
        PREPARE stmt FROM 'DROP TABLE {{ relation.quote(schema=False, identifier=False) }}';
        EXECUTE stmt;
      ELSEIF EXISTS (
        SELECT TABLE_NAME
        FROM QSYS2.TABLES
        WHERE TABLE_NAME = UPPER('{{ relation.identifier }}') AND TABLE_SCHEMA = UPPER('{{ relation.schema }}') AND TABLE_TYPE = 'VIEW'
      ) THEN
        PREPARE stmt FROM 'DROP VIEW {{ relation.quote(schema=False, identifier=False) }}';
        EXECUTE stmt;
      END IF;
    END

    {%- endcall %}
{% endmacro %}


{% macro ibm_dbt__current_timestamp() -%}
  CURRENT_TIMESTAMP
{%- endmacro %}


{% macro ibm_dbt__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = 'DBT_TMP__' ~ base_relation.identifier %}
    {% set tmp_relation = base_relation.incorporate(path={"identifier": tmp_identifier}) -%}
    {% do return(tmp_relation) %}
{% endmacro %}


{% macro ibm_dbt__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        SELECT * FROM (
            {{ select_sql }}
        ) AS dbt_sbq
        WHERE 0=1
        FETCH FIRST 0 ROWS ONLY
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}