{{
    config(
      enabled=True,
      materialized='view', 
    ) 
}}

select 
	coalesce (hs1.customer_pk, hs2.customer_pk) customer_pk,
	coalesce (hs1.customer_key, hs2.customer_key) customer_key,
	coalesce (hs1.LOAD_DATE, hs2.LOAD_DATE) LOAD_DATE,
	coalesce (hs1.EFFECTIVE_FROM , hs2.EFFECTIVE_FROM) EFFECTIVE_FROM,	
	hs1.first_name,
	hs1.last_name,
	hs1.email,
	hs2.country,
	hs2.age
from {{ref ('v_pit_customer_details') }} hs1
full outer join {{ref ('v_pit_customer_crm_details') }} hs2
  on hs1.customer_pk = hs2.customer_pk and  date_trunc('second',hs1.LOAD_DATE) =  date_trunc('second',hs2.LOAD_DATE)