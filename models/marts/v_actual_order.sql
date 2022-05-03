{{
  config(
    enabled=True,
    materialized='view', 
  ) 
}}

with 
hub_order_actual as (
  {{ get_actual('hub_order', ['order_pk', 'order_key'], ['order_pk'], 'LOAD_DATE') }}
),
sat_oreder_details_actual as (
  {{ get_actual('sat_order_details', ['order_pk', 'order_date', 'status'], ['order_pk'], 'LOAD_DATE') }}
)
select * from hub_order_actual left join sat_oreder_details_actual using (order_pk)