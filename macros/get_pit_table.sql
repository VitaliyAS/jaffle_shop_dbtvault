{% macro get_pit_table(source, result_columns, pk_columns, ldts_column) %}
with rank_source as (
    select *, 
    row_number() over ( partition by {{ pk_columns|join(',') }} order by {{ ldts_column }}) as rn
    from {{ ref( source ) }}
)
select 
    {% for col in result_columns %} {{ 'coalesce(rs_cur.%s,rs_prev.%s) as %s, '|format(col,col,col) }} {% endfor %}
    rs_cur.EFFECTIVE_FROM, 
    coalesce(rs_next.EFFECTIVE_FROM, '9999-12-31 00:00:00'::timestamp) as EFFECTIVE_TO
from rank_source rs_cur 
    left join rank_source rs_prev 
        on  rs_cur.rn = rs_prev.rn + 1 
            {% for col in pk_columns %} {{ ' and rs_cur.%s = rs_prev.%s'|format(col, col) }} {% endfor %} 
    left join rank_source rs_next
        on  rs_cur.rn + 1 = rs_next.rn
            {% for col in pk_columns %} {{ ' and rs_cur.%s = rs_next.%s'|format(col, col) }} {% endfor %} 
{% endmacro %}