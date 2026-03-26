{% macro safe_cast_numeric(col) %}
    CASE WHEN {{ col }} ~ '^[0-9]+(\.[0-9]+)?$' THEN {{ col }}::NUMERIC ELSE NULL END
{% endmacro %}

{% macro normalise_date(col) %}
    CASE
        WHEN {{ col }} ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE({{ col }}, 'YYYY-MM-DD')
        WHEN {{ col }} ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE({{ col }}, 'DD-MM-YYYY')
        ELSE NULL
    END
{% endmacro %}
