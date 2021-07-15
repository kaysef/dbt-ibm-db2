{% macro ibmdb2__get_catalog(information_schema, schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}

    WITH columns AS (
      SELECT
        COLUMN_NAME,
        DATA_TYPE,
        TABLE_CATALOG AS DATABASE,
        TABLE_NAME,
        TABLE_SCHEMA,
        ORDINAL_POSITION
      FROM QSYS2.COLUMNS
    ),
    tables AS (
      SELECT
        TABLE_CATALOG AS DATABASE,
        TABLE_SCHEMA,
        TABLE_NAME,
        CASE
          WHEN TABLE_TYPE LIKE '%TABLE%' THEN 'table'
          WHEN TABLE_TYPE = 'VIEW' THEN 'view'
        END AS TABLE_TYPE
      FROM QSYS2.TABLES
      WHERE TABLE_TYPE = 'VIEW' OR TABLE_TYPE LIKE '%TABLE%'
    )
    SELECT
      TRIM(tables.DATABASE) AS "table_database",
      TRIM(tables.TABLE_SCHEMA) AS "table_schema",
      TRIM(tables.TABLE_NAME) AS "table_name",
      tables.TABLE_TYPE AS "table_type",
      NULLIF('','') AS "table_comment",
      TRIM(columns.COLUMN_NAME) AS "column_name",
      columns.ORDINAL_POSITION AS "column_index",
      columns.DATA_TYPE AS "column_type",
      NULLIF('','') AS "column_comment"
    FROM tables
    INNER JOIN columns ON
      columns.DATABASE = tables.DATABASE AND
      columns.TABLE_SCHEMA = tables.TABLE_SCHEMA AND
      columns.TABLE_NAME = tables.TABLE_NAME
    WHERE (
        {%- for schema in schemas -%}
          tables.TABLE_SCHEMA = UPPER('{{ schema }}') {%- if not loop.last %} OR {% endif -%}
        {%- endfor -%}
    )
    ORDER BY
      tables.TABLE_SCHEMA,
      tables.TABLE_NAME,
      columns.ORDINAL_POSITION

  {%- endcall -%}
  {{ return(load_result('catalog').table) }}
{%- endmacro %}