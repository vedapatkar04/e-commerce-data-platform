select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select line_revenue
from "ecommerce_db"."dbt_dev_staging"."stg_orders"
where line_revenue is null



      
    ) dbt_internal_test