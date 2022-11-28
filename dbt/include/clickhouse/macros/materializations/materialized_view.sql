{% macro create_materialized_view_as(relation, target, sql) -%}
    CREATE MATERIALIZED VIEW {{ relation }} TO {{ target }} AS (
        {{ sql }}
    )
{% endmacro %}



{% materialization materialized_view, adapter='clickhouse' -%}
    {% set target_table_exists, target_table = get_or_create_relation(database=this.database, schema=this.schema, identifier=this.identifier, type='table') -%}
    {% set existing_target_table = load_cached_relation(target_table) %}

	{% set prefix ='_mv_' %}
	{% set mv_identifier = prefix ~ this.identifier %}
    {% set target_matview = this.incorporate(path={"identifier": mv_identifier}) %}
    {% set existing_matview = load_cached_relation(target_matview) %}

    {% set tmp_relation = make_intermediate_relation(target_table) %}
    {% do drop_relation_if_exists(tmp_relation) %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set to_drop = [] %}
    {% do create_temporary_empty_table(tmp_relation, sql) %}
    {{ to_drop.append(tmp_relation) }}

    {% if existing_matview is not none %}
        -- проверяем консистентность если матвьюха  существует
        {% set schema_changes_dict = check_for_schema_changes(tmp_relation, existing_matview) %}
        {% if schema_changes_dict['schema_changed'] or existing_target_table is none %}
            {% set full_rebuild = True %}
        {% endif %}
        {% do log("MV exists, schema_changed=" ~ schema_changes_dict['schema_changed'] ~ ", existing_target_table=" ~ existing_target_table, True) %}

    {% else %}
        -- матвьюхи нет, нужно создать
        {% if existing_target_table is not none %}
            -- таргет есть, проверяем консистентность
            {% set schema_changes_dict = check_for_schema_changes(tmp_relation, existing_target_table) %}
            {% if schema_changes_dict['schema_changed'] %}
                -- неконсистентен, пересоздаём всё
                {% set full_rebuild = True %}
            {% endif %}
            -- если ребилд не нужен, создаем матвьюху потому что сейчас она none
            {% set create_matview = True %}
            {% do log("target exists, schema_changed=" ~ schema_changes_dict['schema_changed'], True) %}

        {% else %}
            -- таргета нет, пересоздаём всё
            {% set full_rebuild = True %}
            {% do log("target doesn't exist, full build", True) %}
        {% endif %}

    {% endif %}

    {% if full_rebuild %}
        {% do materialize_table(target_table, sql) %}
    {% endif %}

    {% if full_rebuild or create_matview %}
        {% do materialize_matview(target_matview, target_table, sql) %}

        {{ run_hooks(post_hooks, inside_transaction=True) }}

        {% do persist_docs(target_relation, model) %}

        -- `COMMIT` happens here
        {% do adapter.commit() %}

    {% else %}
        -- for dry run
        {{ store_result('main', 'SKIP') }}
    {% endif %}

    {% for rel in to_drop %}
        {% do adapter.drop_relation(rel) %}
    {% endfor %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_table]}) }}

{%- endmaterialization %}