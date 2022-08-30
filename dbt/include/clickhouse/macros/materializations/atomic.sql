{% macro is_engine_atomic(rel) %}
	{% if not execute %}
		{{ return(None) }}
	{% endif %}

  {%- if rel -%}
      {% set relation = adapter.get_relation(rel.database, rel.schema, rel.table) %}
  {%- else -%}
      {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
  {%- endif -%}

	{% if relation is none %}
		{{ return(None) }}
	{% endif %}

  {% set sel %} 
		( SELECT engine FROM system.databases WHERE name='{{relation.schema}}' )
	{% endset %}
	{% set results = run_query(sel) %}
	{% set engine = results.columns[0].values()[0] %}
  
  {% do return(engine=='Atomic') %}
{% endmacro %}

{% macro exchange_tables_atomic(intermediate_relation, target_relation) %}
  {%- call statement('exchange_tables_atomic') -%}
    EXCHANGE TABLES {{ intermediate_relation }} AND {{ target_relation }}
  {% endcall %}
{% endmacro %}
