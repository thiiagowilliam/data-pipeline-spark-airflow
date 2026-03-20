{% macro mask_partial(column_name, visible_prefix=4, visible_suffix=4) %}
    CASE
        WHEN LENGTH({{ column_name }}) <= {{ visible_prefix }} + {{ visible_suffix }} THEN '***'
        ELSE CONCAT(
            LEFT({{ column_name }}, {{ visible_prefix }}),
            '***',
            RIGHT({{ column_name }}, {{ visible_suffix }})
        )
    END
{% endmacro %}
