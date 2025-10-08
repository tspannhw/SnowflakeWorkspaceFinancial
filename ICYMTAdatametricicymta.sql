/*
********************************************************************************************
* Get started with data quality monitoring. This Worksheet will walk you through how to:
*   - Set up roles and objects with the required access for data quality monitoring.
*   - Define a data metric function (DMF) schedule and setup associations and expectations.
*   - Create a custom DMF to measure data quality.
********************************************************************************************
* Access control setup
********************************************************************************************
*/

-- Ask your account admin to grant the following privileges to set up and review data metric functions (DMF).

grant execute data metric function on account to role ACCOUNTADMIN;
grant database role snowflake.data_metric_user to role ACCOUNTADMIN;

grant usage on database DEMO to role ACCOUNTADMIN;
grant usage on schema DEMO.DEMO to role ACCOUNTADMIN;
grant create data metric function on schema DEMO.DEMO to role ACCOUNTADMIN;

/*
******************************************************************************
* Define schedule and add DMF association and expectation to object
******************************************************************************
*/

-- Define schedule on a target object (i.e. table, dynamic table, view) to run DMFs.

use DEMO.DEMO;

ALTER ICEBERG TABLE ICYMTA SET DATA_METRIC_SCHEDULE = '5 minutes';

describe table icymta;
select * from icymta;

-- Associate DMFs for row count, freshness, null count and add an expectation.
-- Feel free to adjust and use other DMFs as listed here: https://docs.snowflake.com/en/user-guide/data-quality-system-dmfs#system-dmfs

ALTER ICEBERG TABLE ICYMTA
  ADD DATA METRIC FUNCTION
   SNOWFLAKE.CORE.ROW_COUNT ON (),                   -- Row count (Volume)
    SNOWFLAKE.CORE.NULL_COUNT ON (PROGRESSSTATUS)    -- Null count
        EXPECTATION my_exp ( VALUE < 5 );
        
/*
******************************************************************************
* Congrats! You have successfully set up your first DMFs.
* Navigate to the table data quality page to review DMF results.
******************************************************************************
*/

SELECT SNOWFLAKE.CORE.FRESHNESS(
  SELECT
      TO_DATE(EXPECTEDDEPARTURETIME)
  FROM DEMO.DEMO.ICYMTA
) < 300;

select EXPECTEDDEPARTURETIME  FROM DEMO.DEMO.ICYMTA;