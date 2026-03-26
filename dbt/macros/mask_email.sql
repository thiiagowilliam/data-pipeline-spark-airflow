{% macro mask_email(column_name) -%}
    case
        when {{ column_name }} is null then null
        else sha256(trim({{ column_name }}))
    end
{%- endmacro %}