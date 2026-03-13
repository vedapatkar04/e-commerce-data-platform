select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select event_id
from "ecommerce_db"."dbt_dev_staging"."stg_clickstream"
where event_id is null



      
    ) dbt_internal_test