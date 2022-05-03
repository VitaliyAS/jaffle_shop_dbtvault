{{
    config(
      enabled=True,
      materialized='view', 
    ) 
}}

select 
  hc.customer_pk,
  hc.customer_key,
  greatest(hc.load_date, sc2.LOAD_DATE) as LOAD_DATE,
  sc2.EFFECTIVE_FROM as EFFECTIVE_FROM,
  sc2.country,
  sc2.age
from {{ref ('hub_customer') }} hc
full outer join {{ref ('sat_customer_crm_details')}} sc2 
  on hc.customer_pk = sc2.customer_pk and date_trunc('second',hc.LOAD_DATE) <= date_trunc('second',sc2.LOAD_DATE)