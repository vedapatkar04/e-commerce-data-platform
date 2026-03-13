select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        price_band as value_field,
        count(*) as n_records

    from "ecommerce_db"."dbt_dev_staging"."stg_products"
    group by price_band

)

select *
from all_values
where value_field not in (
    'budget','mid','premium'
)



      
    ) dbt_internal_test