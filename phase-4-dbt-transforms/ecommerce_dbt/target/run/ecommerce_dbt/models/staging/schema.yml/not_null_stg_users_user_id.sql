select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select user_id
from "ecommerce_db"."dbt_dev_staging"."stg_users"
where user_id is null



      
    ) dbt_internal_test