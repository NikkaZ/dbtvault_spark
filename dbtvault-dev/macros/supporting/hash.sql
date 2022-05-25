{%- macro hash(columns=none, alias=none, is_hashdiff=false) -%}

    {%- if is_hashdiff is none -%}
        {%- set is_hashdiff = false -%}
    {%- endif -%}

    {{- adapter.dispatch('hash', 'dbtvault')(columns=columns, alias=alias, is_hashdiff=is_hashdiff) -}}

{%- endmacro %}

{%- macro default__hash(columns, alias, is_hashdiff) -%}

{%- set hash = var('hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{#- Select hashing algorithm -#}
{%- if hash == 'MD5' -%}
    {%- set hash_alg = 'MD5_BINARY' -%}
    {%- set hash_size = 16 -%}
{%- elif hash == 'SHA' -%}
    {%- set hash_alg = 'SHA2_BINARY' -%}
    {%- set hash_size = 32 -%}
{%- else -%}
    {%- set hash_alg = 'MD5_BINARY' -%}
    {%- set hash_size = 16 -%}
{%- endif -%}

{%- set standardise = "NULLIF(UPPER(TRIM(CAST([EXPRESSION] AS VARCHAR))), '')" %}

{#- Alpha sort columns before hashing if a hashdiff -#}
{%- if is_hashdiff and dbtvault.is_list(columns) -%}
    {%- set columns = columns|sort -%}
{%- endif -%}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set column_str = dbtvault.as_constant(columns) -%}
<<<<<<< HEAD
    {{- "CAST(({}({})) AS BINARY({})) AS {}".format(hash_alg, standardise | replace('[EXPRESSION]', column_str), hash_size, alias) | indent(4) -}}
=======
    {%- if dbtvault.is_expression(column_str) -%}
        {%- set escaped_column_str = column_str -%}
    {%- else -%}
        {%- set escaped_column_str = dbtvault.escape_column_names(column_str) -%}
    {%- endif -%}
    {{- "CAST(({}({})) AS BINARY({})) AS {}".format(hash_alg, standardise | replace('[EXPRESSION]', escaped_column_str), hash_size, dbtvault.escape_column_names(alias)) | indent(4) -}}
>>>>>>> dbtvault_update

{#- Else a list of columns to hash -#}
{%- else -%}
    {%- set all_null = [] -%}

    {%- if is_hashdiff -%}
        {{- "CAST({}(CONCAT_WS('{}',".format(hash_alg, concat_string) | indent(4) -}}
    {%- else -%}
        {{- "CAST({}(NULLIF(CONCAT_WS('{}',".format(hash_alg, concat_string) | indent(4) -}}
    {%- endif -%}

    {%- for column in columns -%}

        {%- do all_null.append(null_placeholder_string) -%}

        {%- set column_str = dbtvault.as_constant(column) -%}
<<<<<<< HEAD
        {{- "\nIFNULL({}, '{}')".format(standardise | replace('[EXPRESSION]', column_str), null_placeholder_string) | indent(4) -}}
=======
        {%- if dbtvault.is_expression(column_str) -%}
            {%- set escaped_column_str = column_str -%}
        {%- else -%}
            {%- set escaped_column_str = dbtvault.escape_column_names(column_str) -%}
        {%- endif -%}
        {{- "\nIFNULL({}, '{}')".format(standardise | replace('[EXPRESSION]', escaped_column_str), null_placeholder_string) | indent(4) -}}
>>>>>>> dbtvault_update
        {{- "," if not loop.last -}}

        {%- if loop.last -%}

            {% if is_hashdiff %}
<<<<<<< HEAD
                {{- "\n)) AS BINARY({})) AS {}".format(hash_size, alias) -}}
            {%- else -%}
                {{- "\n), '{}')) AS BINARY({})) AS {}".format(all_null | join(""), hash_size, alias) -}}
=======
                {{- "\n)) AS BINARY({})) AS {}".format(hash_size, dbtvault.escape_column_names(alias)) -}}
            {%- else -%}
                {{- "\n), '{}')) AS BINARY({})) AS {}".format(all_null | join(""), hash_size, dbtvault.escape_column_names(alias)) -}}
>>>>>>> dbtvault_update
            {%- endif -%}
        {%- else -%}

            {%- do all_null.append(concat_string) -%}

        {%- endif -%}

    {%- endfor -%}

{%- endif -%}

{%- endmacro -%}

{%- macro bigquery__hash(columns, alias, is_hashdiff) -%}

{%- set hash = var('hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{#- Select hashing algorithm -#}

{%- if hash == 'MD5' -%}
    {%- set hash_alg = 'MD5' -%}
{%- elif hash == 'SHA' -%}
    {%- set hash_alg = 'SHA256' -%}
{%- endif -%}

{%- set standardise = "NULLIF(UPPER(TRIM(CAST([EXPRESSION] AS STRING))), '')" %}

{#- Alpha sort columns before hashing if a hashdiff -#}
{%- if is_hashdiff and dbtvault.is_list(columns) -%}
    {%- set columns = columns|sort -%}
{%- endif -%}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set column_str = dbtvault.as_constant(columns) -%}
<<<<<<< HEAD
    {{- "CAST(UPPER(TO_HEX({}({}))) AS STRING) AS {}".format(hash_alg, standardise | replace('[EXPRESSION]', column_str), alias) | indent(4) -}}
=======
    {%- if dbtvault.is_expression(column_str) -%}
        {%- set escaped_column_str = column_str -%}
    {%- else -%}
        {%- set escaped_column_str = dbtvault.escape_column_names(column_str) -%}
    {%- endif -%}
    {{- "CAST(UPPER(TO_HEX({}({}))) AS STRING) AS {}".format(hash_alg, standardise | replace('[EXPRESSION]', escaped_column_str), dbtvault.escape_column_names(alias)) | indent(4) -}}
>>>>>>> dbtvault_update

{#- Else a list of columns to hash -#}
{%- else -%}
 {%- set all_null = [] -%}
    {%- if is_hashdiff -%}
        {{- "UPPER(TO_HEX({}(CONCAT(".format(hash_alg) | indent(4) -}}

    {%- else -%}
        {{- "UPPER(TO_HEX({}(NULLIF(CONCAT(".format(hash_alg) | indent(4) -}}
    {%- endif -%}

    {%- for column in columns -%}

        {%- do all_null.append(null_placeholder_string) -%}

        {%- set column_str = dbtvault.as_constant(column) -%}
<<<<<<< HEAD
        {{- "\nIFNULL({}, '{}')".format(standardise | replace('[EXPRESSION]', column_str), null_placeholder_string) | indent(4) -}}
=======
        {%- if dbtvault.is_expression(column_str) -%}
            {%- set escaped_column_str = column_str -%}
        {%- else -%}
            {%- set escaped_column_str = dbtvault.escape_column_names(column_str) -%}
        {%- endif -%}
        {{- "\nIFNULL({}, '{}')".format(standardise | replace('[EXPRESSION]', escaped_column_str), null_placeholder_string) | indent(4) -}}
>>>>>>> dbtvault_update
        {{- ",'{}',".format(concat_string) if not loop.last -}}
        {%- if loop.last -%}

            {% if is_hashdiff %}
<<<<<<< HEAD
                {{- "\n)))) AS {}".format(alias) -}}
            {%- else -%}
                {{- "\n), '{}')))) AS {}".format(all_null | join(""), alias) -}}
=======
                {{- "\n)))) AS {}".format(dbtvault.escape_column_names(alias)) -}}
            {%- else -%}
                {{- "\n), '{}')))) AS {}".format(all_null | join(""), dbtvault.escape_column_names(alias)) -}}
>>>>>>> dbtvault_update
            {%- endif -%}
        {%- else -%}

            {%- do all_null.append(concat_string) -%}
        {%- endif -%}

    {%- endfor -%}

{%- endif -%}

{%- endmacro -%}

{%- macro sqlserver__hash(columns, alias, is_hashdiff) -%}

{%- set hash = var('hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{#- Select hashing algorithm -#}
{%- if hash == 'MD5' -%}
    {%- set hash_alg = 'MD5' -%}
    {%- set hash_size = 16 -%}
{%- elif hash == 'SHA' -%}
    {%- set hash_alg = 'SHA2_256' -%}
    {%- set hash_size = 32 -%}
{%- else -%}
    {%- set hash_alg = 'MD5' -%}
    {%- set hash_size = 16 -%}
{%- endif -%}

{%- set standardise = "NULLIF(UPPER(TRIM(CAST([EXPRESSION] AS VARCHAR(max)))), '')" %}

{#- Alpha sort columns before hashing if a hashdiff -#}
{%- if is_hashdiff and dbtvault.is_list(columns) -%}
    {%- set columns = columns|sort -%}
{%- endif -%}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set column_str = dbtvault.as_constant(columns) -%}
<<<<<<< HEAD
    {{- "CAST(HASHBYTES('{}', {}) AS BINARY({})) AS {}".format(hash_alg, standardise | replace('[EXPRESSION]', column_str), hash_size, alias) | indent(4) -}}
=======
    {%- if dbtvault.is_expression(column_str) -%}
        {%- set escaped_column_str = column_str -%}
    {%- else -%}
        {%- set escaped_column_str = dbtvault.escape_column_names(column_str) -%}
    {%- endif -%}
    {{- "CAST(HASHBYTES('{}', {}) AS BINARY({})) AS {}".format(hash_alg, standardise | replace('[EXPRESSION]', escaped_column_str), hash_size, dbtvault.escape_column_names(alias)) | indent(4) -}}
>>>>>>> dbtvault_update

{#- Else a list of columns to hash -#}
{%- else -%}
    {%- set all_null = [] -%}

    {%- if is_hashdiff -%}
        {{- "CAST(HASHBYTES('{}', (CONCAT_WS('{}',".format(hash_alg, concat_string) | indent(4) -}}
    {%- else -%}
        {{- "CAST(HASHBYTES('{}', (NULLIF(CONCAT_WS('{}',".format(hash_alg, concat_string) | indent(4) -}}
    {%- endif -%}

    {%- for column in columns -%}

        {%- do all_null.append(null_placeholder_string) -%}

        {%- set column_str = dbtvault.as_constant(column) -%}
<<<<<<< HEAD
        {{- "\nISNULL({}, '{}')".format(standardise | replace('[EXPRESSION]', column_str), null_placeholder_string) | indent(4) -}}
        {{- "," if not loop.last -}}

        {%- if loop.last -%}

            {% if is_hashdiff %}
                {{- "\n))) AS BINARY({})) AS {}".format(hash_size, alias) -}}
            {%- else -%}
                {{- "\n), '{}'))) AS BINARY({})) AS {}".format(all_null | join(""), hash_size, alias) -}}
            {%- endif -%}
        {%- else -%}

            {%- do all_null.append(concat_string) -%}

        {%- endif -%}

    {%- endfor -%}

{%- endif -%}

{%- endmacro -%}

{%- macro spark__hash(columns, alias, is_hashdiff) -%}

{%- set hash = var('hash', 'MD5') -%}
{%- set concat_string = var('concat_string', '||') -%}
{%- set null_placeholder_string = var('null_placeholder_string', '^^') -%}

{#- Select hashing algorithm -#}
{%- if hash == 'MD5' -%}
    {%- set hash_alg = 'MD5' -%}
    {%- set hash_size = '' -%}
{%- elif hash == 'SHA' -%}
    {%- set hash_alg = 'SHA2' -%}
    {%- set hash_size = ',256' -%}
{%- else -%}
    {%- set hash_alg = 'MD5' -%}
    {%- set hash_size = '' -%}
{%- endif -%}

{%- set standardise = "NULLIF(UPPER(TRIM(CAST([EXPRESSION] AS STRING))), '')" %}

{#- Alpha sort columns before hashing if a hashdiff -#}
{%- if is_hashdiff and dbtvault.is_list(columns) -%}
    {%- set columns = columns|sort -%}
{%- endif -%}

{#- If single column to hash -#}
{%- if columns is string -%}
    {%- set column_str = dbtvault.as_constant(columns) -%}
    {{- "CAST({}({} {}) AS BINARY) AS {}".format(hash_alg, standardise | replace('[EXPRESSION]', column_str), hash_size, alias) | indent(4) -}}
{#- Else a list of columns to hash -#}
{%- else -%}
    {%- set all_null = [] -%}

    {%- if is_hashdiff -%}
        {{- "CAST({}(CONCAT_WS('{}',".format(hash_alg, concat_string) | indent(4) -}}
    {%- else -%}
        {{- "CAST({}(NULLIF(CONCAT_WS('{}',".format(hash_alg, concat_string) | indent(4) -}}
    {%- endif -%}

    {%- for column in columns -%}

        {%- do all_null.append(null_placeholder_string) -%}

        {%- set column_str = dbtvault.as_constant(column) -%}
        {{- "\nIFNULL({}, '{}')".format(standardise | replace('[EXPRESSION]', column_str), null_placeholder_string) | indent(4) -}}
        {{- "," if not loop.last -}}

        {%- if loop.last -%}
            {% if is_hashdiff %}
                {{- "\n) {}) AS BINARY) AS {}".format(hash_size, alias) -}}
            {%- else -%}
                {{- "\n), '{}'){}) AS BINARY) AS {}".format(all_null | join(""), hash_size, alias) -}}
=======
        {%- if dbtvault.is_expression(column_str) -%}
            {%- set escaped_column_str = column_str -%}
        {%- else -%}
            {%- set escaped_column_str = dbtvault.escape_column_names(column_str) -%}
        {%- endif -%}
        {{- "\nISNULL({}, '{}')".format(standardise | replace('[EXPRESSION]', escaped_column_str), null_placeholder_string) | indent(4) -}}
        {{- "," if not loop.last -}}

        {%- if loop.last -%}

            {% if is_hashdiff %}
                {{- "\n))) AS BINARY({})) AS {}".format(hash_size, dbtvault.escape_column_names(alias)) -}}
            {%- else -%}
                {{- "\n), '{}'))) AS BINARY({})) AS {}".format(all_null | join(""), hash_size, dbtvault.escape_column_names(alias)) -}}
>>>>>>> dbtvault_update
            {%- endif -%}
        {%- else -%}

            {%- do all_null.append(concat_string) -%}

        {%- endif -%}

    {%- endfor -%}

{%- endif -%}

{%- endmacro -%}