{% macro get_query_results_as_dict(query) %}
    {{ return(adapter.dispatch('get_query_results_as_dict', 'dbtvault')(query)) }}
{% endmacro %}

{% macro default__get_query_results_as_dict(query) %}
<<<<<<< HEAD

{# This macro returns a dictionary of the form {column_name: (tuple_of_results)} #}
=======
    {{ return(dbt_utils.get_query_results_as_dict(query)) }}
{% endmacro %}

{% macro sqlserver__get_query_results_as_dict(query) %}
>>>>>>> dbtvault_update

    {%- call statement('get_query_results', fetch_result=True,auto_begin=false) -%}

        {{ query }}

    {%- endcall -%}

    {% set sql_results={} %}

    {%- if execute -%}
        {% set sql_results_table = load_result('get_query_results').table.columns %}
        {% for column_name, column in sql_results_table.items() %}
            {# Column names in upper case for consistency #}
            {% do sql_results.update({column_name.upper(): column.values()}) %}
        {% endfor %}
    {%- endif -%}

    {{ return(sql_results) }}

<<<<<<< HEAD
{% endmacro %}
=======
{% endmacro %}
>>>>>>> dbtvault_update
