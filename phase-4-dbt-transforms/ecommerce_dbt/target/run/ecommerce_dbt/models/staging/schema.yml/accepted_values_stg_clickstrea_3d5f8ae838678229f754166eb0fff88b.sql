select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from "ecommerce_db"."dbt_dev_staging"."stg_clickstream"
    group by event_type

)

select *
from all_values
where value_field not in (
    'page_view','product_view','add_to_cart','remove_from_cart','checkout','purchase'
)



      
    ) dbt_internal_test