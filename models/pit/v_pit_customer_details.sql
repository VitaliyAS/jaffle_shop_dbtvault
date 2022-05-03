{{
    config(
      enabled=True,
      materialized='view', 
    ) 
}}

select 
  hc.customer_pk,
  hc.customer_key,
  greatest(hc.load_date, sc1.LOAD_DATE) as LOAD_DATE,
  sc1.EFFECTIVE_FROM as EFFECTIVE_FROM,
  sc1.first_name,
  sc1.last_name,
  sc1.email
from {{ref ('hub_customer') }} hc
full outer join {{ref ('sat_customer_details')}} sc1
  on hc.customer_pk = sc1.customer_pk and date_trunc('second',hc.LOAD_DATE) <= date_trunc('second',sc1.LOAD_DATE)