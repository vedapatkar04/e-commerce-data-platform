
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from "ecommerce_db"."dbt_dev_staging"."stg_orders"
    group by order_status

)

select *
from all_values
where value_field not in (
    'pending','processing','shipped','delivered','cancelled'
)


