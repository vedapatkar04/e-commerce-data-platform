select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select country
from "ecommerce_db"."dbt_dev_staging"."stg_users"
where country is null



      
    ) dbt_internal_test