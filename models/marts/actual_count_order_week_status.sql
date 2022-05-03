{{
  config(
    enabled=True,
  ) 
}}
select date_trunc('week', order_date) as order_week, status, count(*) from {{ ref( 'v_actual_order' )}} group by date_trunc('week', order_date), status