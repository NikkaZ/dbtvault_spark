{%- macro type_timestamp() -%}
  {{ return(adapter.dispatch('type_timestamp', 'dbtvault')()) }}
{%- endmacro -%}

<<<<<<< HEAD
{% macro default__type_timestamp() %}
    {{ dbt_utils.type_timestamp() }}
{% endmacro %}

{% macro sqlserver__type_timestamp() %}
    datetime
{% endmacro %}
=======
{%- macro default__type_timestamp() -%}
    {{ dbt_utils.type_timestamp() }}
{%- endmacro -%}

{%- macro sqlserver__type_timestamp() -%}
    datetime2
{%- endmacro -%}
>>>>>>> dbtvault_update
