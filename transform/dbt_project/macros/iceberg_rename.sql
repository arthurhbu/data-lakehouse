{#
  DuckDB Iceberg REST catalogs require CTAS and ALTER TABLE ... RENAME to run
  in separate transactions. This project-level override keeps dbt standard
  table materialization while committing after the temporary table is created.

  Remove this macro when the equivalent upstream dbt-duckdb fix is released
  and adopted by this project.
#}
{% macro duckdb__rename_relation(from_relation, to_relation) -%}

  {% if from_relation.database | lower == target.database | lower %}
    {% do adapter.commit() %}
  {% endif %}

  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}

  {% call statement('rename_relation') -%}
    alter {{ to_relation.type }} {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}

{%- endmacro %}
