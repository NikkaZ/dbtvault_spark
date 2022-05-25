{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ dbtvault_test.get_schema_name() }}
{%- endmacro %}


{% macro get_schema_name() -%}
    {{- adapter.dispatch('get_schema_name', 'dbtvault_test')() -}}
{%- endmacro -%}

{% macro default__get_schema_name() -%}

<<<<<<< HEAD
    {%- set schema_name = "{}_{}{}".format(target.schema, target.user, pipeline_string) -%}
=======
    {%- set schema_name = "{}_{}{}".format(target.schema, target.user, dbtvault_test.pipeline_string()) -%}
>>>>>>> dbtvault_update

    {% do return(clean_schema_name(schema_name)) %}

{%- endmacro -%}

{%- macro bigquery__get_schema_name() -%}

<<<<<<< HEAD
    {%- set schema_name = "{}_{}{}".format(target.dataset, target.project, pipeline_string)  -%}
=======
    {%- set schema_name = "{}_{}{}".format(target.dataset, target.project, dbtvault_test.pipeline_string()) -%}
>>>>>>> dbtvault_update

    {% do return(clean_schema_name(schema_name)) %}

{%- endmacro %}

{%- macro sqlserver__get_schema_name() -%}

<<<<<<< HEAD
    {%- set schema_name = "{}_{}{}".format(target.schema, target.user, pipeline_string) -%}
=======
    {%- set schema_name = "{}_{}{}".format(target.schema, target.user, dbtvault_test.pipeline_string()) -%}

    {% do return(clean_schema_name(schema_name)) %}

{%- endmacro %}

{%- macro spark__get_schema_name() -%}

    {%- set schema_name = "{}_{}".format(target.schema, target.name, dbtvault_test.pipeline_string()) -%}
>>>>>>> dbtvault_update

    {% do return(clean_schema_name(schema_name)) %}

{%- endmacro %}

{%- macro clean_schema_name(schema_name) -%}

<<<<<<< HEAD
    {%- do return(schema_name | replace('-','_') | replace('.','_') | replace('/','_')) -%}
=======
    {%- do return(schema_name | replace('-','_') | replace('.','_') | replace('/','_') | trim | upper) -%}
>>>>>>> dbtvault_update

{%- endmacro -%}


<<<<<<< HEAD
{%- macro append_pipeline_string() -%}

    {%- set pipeline_string -%}
        {{- '_' ~ env_var('CIRCLE_BRANCH', '') | replace('-','_') | replace('.','_') | replace('/','_') if env_var('CIRCLE_BRANCH', '') -}}
        {{- '_' ~ env_var('CIRCLE_JOB', '') if env_var('CIRCLE_JOB', '') -}}
        {{- '_' ~ env_var('CIRCLE_NODE_INDEX', '') if env_var('CIRCLE_NODE_INDEX', '') -}}
    {%- endset -%}

    {% do return(pipeline_string) %}
=======
{%- macro pipeline_string() -%}

    {%- set pipeline_str -%}
        {{- '_' ~ env_var('PIPELINE_BRANCH', '') | replace('-','_') | replace('.','_') | replace('/','_') | replace(' ','_') if env_var('PIPELINE_BRANCH', '') -}}
        {{- '_' ~ env_var('PIPELINE_JOB', '') | replace('-','_') | replace('.','_') | replace('/','_') | replace(' ','_') if env_var('PIPELINE_JOB', '') -}}
    {%- endset -%}

    {% do return(pipeline_str | upper) %}
>>>>>>> dbtvault_update

{%- endmacro -%}