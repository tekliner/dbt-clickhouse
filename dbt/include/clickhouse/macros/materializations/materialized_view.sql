{% macro create_materialized_view_as(relation, target, sql) -%}
    CREATE MATERIALIZED VIEW {{ relation }} TO {{ target }} AS (
        {{ sql }}
    )
{% endmacro %}


{% materialization materialized_view, adapter='clickhouse' -%}
    {% set target_mv_table = config.require('target_mv_table') %}

    {% set target_matview = this %}
    {% set existing_matview = load_cached_relation(this) %}

    {% set mv_target_relation_exists, mv_target_relation = get_or_create_relation(database=this.database,schema=this.schema,identifier=target_mv_table, type='table') -%}
    {% set existing_mv_target = load_cached_relation(mv_target_relation) %}

    {% set tmp_relation = make_intermediate_relation(mv_target_relation) %}
    {% do drop_relation_if_exists(tmp_relation) %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set to_drop = [] %}
    {% do create_temporary_empty_table(tmp_relation, sql) %}

    {% if existing_matview is not none %}
        -- проверяем консистентность если матвьюха  существует
        {% set schema_changes_dict = check_for_schema_changes(tmp_relation, existing_matview) %}
        {% if schema_changes_dict['schema_changed'] or existing_mv_target is none %}
            {% set full_rebuild = True %}
        {% endif %}

    {% else %}
        -- матвьюхи нет, нужно создать
        {% if existing_mv_target is not none %}
            -- таргет есть, проверяем консистентность
            {% set schema_changes_dict = check_for_schema_changes(tmp_relation, existing_mv_target) %}
            {% if schema_changes_dict['schema_changed'] %}
                -- неконсистентен, пересоздаём всё
                {% set full_rebuild = True %}
            {% endif %}
            -- если ребилд не нужен, создаем матвьюху потому что сейчас она none
            {% set create_matview = True %}

        {% else %}
            -- таргета нет, пересоздаём всё
            {% set full_rebuild = True %}
        {% endif %}

    {% endif %}

    {% if full_rebuild %}
        {% do materialize_table(mv_target_relation, sql) %}
    {% endif %}

    {% if full_rebuild or create_matview %}
        {% do materialize_matview(target_matview, mv_target_relation, sql) %}

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

    {{ return({'relations': [target_matview]}) }}

{%- endmaterialization %}