{% macro regex_replace_schema(raw_sql, base_relation, target_relation) %}
    {% set re = modules.re %}

    {% set relation_pattern = base_relation.identifier %}
    {% set match = re.search(relation_pattern, raw_sql) %}
    {% if not match %}
        {% do exceptions.raise_compiler_error(raw_schema ~ ' table not found in raw sql for replace') %}
    {% endif %}

    {% set new_sql = raw_sql.replace(match.group(), target_relation.schema + '.' + target_relation.identifier) %}
    {% do return(new_sql) %}
{% endmacro %}


{% macro show_create_table(relation) %}
    {% call statement('show_create_table', fetch_result=True) %}
        SHOW CREATE TABLE {{ relation.schema }}.{{ relation.identifier }}
    {% endcall %}

    {% do return(load_result('show_create_table').table.columns['statement'][0]) %}
{% endmacro %}


{% materialization dictionary, adapter='clickhouse' %}
    {% set target_relation = this.incorporate(type='table', table_engine="Dictionary", drop_type="dictionary") %}
    {% set existing_relation = load_relation(target_relation) -%}

    {% set intermediate_relation = make_temp_relation(target_relation)-%}

    {% set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
    {% set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

    {% do drop_relation_if_exists(intermediate_relation) %}
    {% do drop_relation_if_exists(backup_relation) %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set to_drop = [] %}
    {% set ddl_changed = False %}

    {% if existing_relation is none %}
        -- No existing dict, simply create a new one
        {% call statement('main') %}
            {{ sql }}
        {% endcall %}

    {% else %}
        {% set re = modules.re %}
        -- Strip whitespace to compare
        {% set old_sql_stripped = re.sub('\s+', ' ', show_create_table(existing_relation).strip()) %}

        {% set sql_fixed = re.sub('(?i)if not exists', '', regex_replace_schema(sql, target_relation, target_relation).strip()) %}
        {% set new_sql_stripped = re.sub('\s+', ' ', sql_fixed) %}

        {% if old_sql_stripped != new_sql_stripped  %}
            {% do log("DDL changed for " ~ existing_relation, True) %}
            {% do log("OLD: " ~ old_sql_stripped, True) %}
            {% do log("NEW: " ~ new_sql_stripped, True) %}

            {% set ddl_changed = True %}
        {% endif %}

        {% if ddl_changed %}
            {% if existing_relation.can_exchange %}
                -- We can do an atomic exchange, so no need for an intermediate
                {% set build_sql = regex_replace_schema(sql, target_relation, backup_relation) %}
                {% call statement('main') %}
                    {{ build_sql }}
                {% endcall %}
                {% do exchange_tables_atomic(backup_relation, existing_relation) %}

            {% else %}
                -- We have to use an intermediate and rename accordingly
                {% set build_sql = regex_replace_schema(sql, target_relation, intermediate_relation) %}
                {% call statement('main') %}
                    {{ build_sql }}
                {% endcall %}

                {{ adapter.rename_relation(existing_relation, backup_relation) }}
                {{ adapter.rename_relation(intermediate_relation, target_relation) }}

                {{ to_drop.append(intermediate_relation) }}
            {% endif %}

            {{ to_drop.append(backup_relation) }}
        {% else %}
            {% set do_nothing = True %}
        {% endif %}

    {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {% do persist_docs(target_relation, model) %}
    -- `COMMIT` happens here
    {% do adapter.commit() %}

    {% for rel in to_drop %}
        {% do adapter.drop_relation(rel) %}
    {% endfor %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% if do_nothing %}
        -- to avoid error when nothing gets done
        {% do log("ddl unchanged, skipping " ~ target_relation, True) %}
        {{ store_result('main', 'SKIP') }}
    {% endif %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}