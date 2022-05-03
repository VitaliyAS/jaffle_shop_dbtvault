{% macro get_actual(source, result_columns, pk_columns, ldts_column) %}
    select {{ result_columns|join(',') }}
    from ( 
        select *, 
        row_number() over ( partition by {{ pk_columns|join(',') }} order by {{ ldts_column }} desc ) as rn
        from {{ ref( source ) }}
    ) as sub 
    where rn = 1
{% endmacro %}