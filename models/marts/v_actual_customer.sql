{{
  config(
    enabled=True,
    materialized='view', 
  ) 
}}

with 
hub_customer_actual as (
  {{ get_actual('hub_customer', ['CUSTOMER_PK', 'customer_key'], ['CUSTOMER_PK'], 'LOAD_DATE') }}
),
sat_customer_details_actual as (
  {{ get_actual('sat_customer_details', ['CUSTOMER_PK', 'first_name', 'last_name', 'email'], ['CUSTOMER_PK'], 'LOAD_DATE') }}
)
select * from hub_customer_actual left join sat_customer_details_actual using (CUSTOMER_PK)