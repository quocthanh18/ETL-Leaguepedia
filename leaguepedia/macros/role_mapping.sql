{% macro map_role(position) %}
    CASE {{ position}}
        WHEN 1 THEN 'top'
        WHEN 2 THEN 'jungle'
        WHEN 3 THEN 'mid'
        WHEN 4 THEN 'ad'
        WHEN 5 THEN 'sp'
    END
{% endmacro %}
