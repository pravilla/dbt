{% macro snowflake__create_table_as(temporary, relation, sql) -%}
  {% if temporary %}
    use schema {{ adapter.quote_as_configured(schema, 'schema') }};
  {% endif %}

  {{ default__create_table_as(temporary, relation, sql) }}
{% endmacro %}

{% macro snowflake__create_view_as(relation, sql) -%}
  create or replace view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}

{% macro snowflake__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from
      {{ information_schema_name(relation.database) }}.columns

      where table_name ilike '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema ilike '{{ relation.schema }}'
        {% endif %}
        {% if relation.database %}
        and table_catalog ilike '{{ relation.database }}'
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}

{% endmacro %}


{% macro snowflake__list_relations_without_caching(database, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as database,
      table_name as name,
      table_schema as schema,
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type
    from {{ information_schema_name(database) }}.tables
    where table_schema ilike '{{ schema }}'
      and table_catalog ilike '{{ database }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro snowflake__information_schema_name(database) -%}
  {%- if database -%}
    "{{ database }}".information_schema
  {%- else -%}
    information_schema
  {%- endif -%}
{%- endmacro %}


{% macro snowflake__check_schema_exists(database, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True) -%}
        select count(*)
        from {{ information_schema_name(database) }}
        where upper(schema_name) = upper('{{ schema }}')
            and upper(catalog_name) = upper('{{ database }}')
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{%- endmacro %}
