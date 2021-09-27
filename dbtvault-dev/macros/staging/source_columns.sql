{%- macro source_columns(source_relation=none) -%}

    {%- if source_relation -%}
        {%- set source_model_cols = adapter.get_columns_in_relation(source_relation) -%}

        {%- set column_list = [] -%}

        {%- for source_col in source_model_cols -%}
            {%- do column_list.append(source_col.column) -%}
        {%- endfor -%}

        {%- do return(column_list) -%}
    {%- endif %}

{%- endmacro -%}

{%- macro source_columns_from_selected(source_relation=none,selected_columns=none) -%}

    {%- if source_relation -%}
        {%- set source_model_cols = adapter.get_columns_in_relation(source_relation) -%}
        {%- set column_list = [] -%}

        {%- for source_col in source_model_cols -%}
            {% if source_col.column in selected_columns %}
                {%- do column_list.append(source_col.column) -%}
            {%- endif %}
        {%- endfor -%}

        {%- do return(column_list) -%}
    {%- endif %}

{%- endmacro -%}
