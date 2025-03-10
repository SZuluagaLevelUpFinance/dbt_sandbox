-- back compat for old kwarg name
  
  begin;
    

        insert into DIMS.PUBLIC.orders ("ACCOUNT_ID", "CONTRACT_ID", "CONTRACT_DATE", "CONTRACT_AMOUNT", "PRODUCT_ID", "QUANTITY", "START_DATE", "END_DATE")
        (
            select "ACCOUNT_ID", "CONTRACT_ID", "CONTRACT_DATE", "CONTRACT_AMOUNT", "PRODUCT_ID", "QUANTITY", "START_DATE", "END_DATE"
            from DIMS.PUBLIC.orders__dbt_tmp
        );
    commit;