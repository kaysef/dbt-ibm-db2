{% macro ibm_db2__incremental_upsert(tmp_relation, target_relation, unique_key=none, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='name') | join(', ') -%}

    {%- if unique_key is not none -%}
        MERGE INTO {{ target_relation }} AS target
        USING {{ tmp_relation }} temp ({{ dest_cols_csv }})
        ON
        {% for key in unique_key -%} 
            (temp.{{ key }} = target.{{ key }}){% if not loop.last %} AND {% endif %}
        {%- endfor %}    
        WHEN MATCHED THEN
        UPDATE SET
        {% for col in dest_columns if col.name|lower not in unique_key -%}
            target.{{ col.name }} = temp.{{ col.name }}
            {%- if not loop.last -%}, {% endif -%}
        {%- endfor %}
        WHEN NOT MATCHED THEN
        INSERT ( {{ dest_cols_csv }} )
        VALUES (
            {%- for col in dest_columns -%}
                temp.{{ col.name }}
            {%- if not loop.last -%}, {% endif %}
            {%- endfor -%}
        )
    {%- else -%}
        INSERT INTO {{ target_relation }} ({{ dest_cols_csv }})
        (
        SELECT {{ dest_cols_csv }}
        FROM {{ tmp_relation }}
        )
    {%- endif -%}
{%- endmacro %}




{% macro ibm_db2__incremental_delete_insert(tmp_relation, target_relation, unique_key=none, statement_name="main") %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='name') | join(', ') -%}
 
    BEGIN
        {% if unique_key is not none -%}
            DELETE FROM {{ target_relation }}
            WHERE
                {% for filter in unique_key -%} 
                    {{ filter }}{% if not loop.last %} AND {% endif %}
                {%- endfor -%}
            ;
        {% endif -%}
        INSERT INTO {{ target_relation }} ({{ dest_cols_csv }})
        (
            SELECT {{ dest_cols_csv }}
            FROM {{ tmp_relation }}
        );
    END
{%- endmacro %}




{% macro ibm_db2__incremental_update_insert(tmp_relation, target_relation, unique_key=none, statement_name="main") %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='name') | join(', ') -%}

    {%- set non_key_columns -%}
        {% for col in dest_columns if col.name|lower not in unique_key -%}
            {{ col.name }}{%- if not loop.last -%}, {% endif -%}
        {%- endfor -%}
    {%- endset -%}


    {%- set rows_to_be_inserted_query -%}
        SELECT * FROM {{ tmp_relation }}
        EXCEPT
        SELECT * FROM {{ target_relation }}
    {%- endset -%}
    {%- set insert_length = run_query(rows_to_be_inserted_query).columns[0].values() | length -%}

    {%- set null_to_be_inserted_query %}
        SELECT * FROM {{ tmp_relation }} WHERE crop_year IS NULL
    {%- endset -%}
    {%- set null_length = run_query(null_to_be_inserted_query).columns[0].values() | length -%}

    {{
        log('There are ' ~ insert_length ~ ' rows to be inserted and ' ~ null_length ~ ' null values in crop_year')
    }}

    BEGIN
        {% if unique_key is not none -%}
            UPDATE {{ target_relation }} AS target
                SET ({{ non_key_columns }}) = (
                    SELECT {{ non_key_columns }} FROM {{ tmp_relation }} AS temp
                    WHERE
                        {% for key in unique_key -%} 
                            temp.{{ key }} = target.{{ key }}{% if not loop.last %} AND {% endif %}
                        {%- endfor %}
                );
        {%- endif %}
        {%- if insert_length > 0 %}
            INSERT INTO {{ target_relation }} ({{ dest_cols_csv }}) 
            (
                SELECT * FROM {{ tmp_relation }}
                EXCEPT
                SELECT * FROM {{ target_relation }}
            );
        {%- endif %}
    END
{%- endmacro %}



