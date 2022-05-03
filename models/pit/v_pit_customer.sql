{{
    config(
      enabled=True,
      materialized='view', 
    ) 
}}

{{ get_pit_table('v_pit_customer_all', ['CUSTOMER_PK', 'customer_key', 'first_name', 'last_name', 'email', 'country', 'age'], ['CUSTOMER_PK'], 'LOAD_DATE') }}