
  
    

        create or replace transient table DIMS.PUBLIC.last_gen
         as
        (

select
	current_timestamp() as last_run_datetime
	, current_date() as last_run_date
	, '2025-08-08'::date as last_gen_date
        );
      
  