{#
  DuckDB does not support DROP TABLE ... CASCADE for Iceberg REST tables.
  This project has no catalog-level dependent objects, so a plain DROP is the
  correct cleanup for dbt temporary and backup tables.

  Remove this macro when the equivalent upstream dbt-duckdb fix is released
  and adopted by this project.
#}
{% macro duckdb__drop_relation(relation) -%}

  {% call statement('drop_relation', auto_begin=False) -%}
    {% if relation.database | lower == target.database | lower %}
      drop {{ relation.type }} if exists {{ relation }}
    {% else %}
      drop {{ relation.type }} if exists {{ relation }} cascade
    {% endif %}
  {%- endcall %}

{%- endmacro %}
