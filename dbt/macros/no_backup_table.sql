{% macro clickhouse__get_replace_table_sql(relation, sql) %}
    {% set sql_header = config.get('sql_header', none) %}

    {{ sql_header if sql_header is not none }}

    DROP TABLE IF EXISTS {{ relation }} SYNC;
    
    CREATE TABLE {{ relation }} AS
    {{ sql }}
{% endmacro %}