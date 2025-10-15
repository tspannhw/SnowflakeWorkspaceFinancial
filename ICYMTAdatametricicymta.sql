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

CREATE OR REPLACE VIEW LTZMTCA(DATE1_LTZ) AS 
SELECT TS::TIMESTAMP_LTZ FROM DEMO.DEMO.ICYMTA WHERE TS IS NOT NULL;


SELECT SNOWFLAKE.CORE.FRESHNESS(
SELECT DATE1_LTZ FROM LTZMTCA) < 300;

  SELECT '2024-01-01 12:00:00'::TIMESTAMP_TZ
  FROM DEMO.DEMO.ICYMTA
  
--  TO_TIMESTAMP_TZ(EXPECTEDDEPARTURETIME, 'YYYYMMDDHH24:MI:SS')::TIMESTAMP_TZ
-- https://community.snowflake.com/s/article/FRESHNESS-function-gives-error-even-after-type-casting-the-column-with-TIMESTAMP-NTZ-datatype-to-allowed-datatype

select EXPECTEDDEPARTURETIME, TO_TIMESTAMP_TZ(EXPECTEDDEPARTURETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM') as TimeStampx , VEHICLEREF, DESTINATIONNAME,TO_TIMESTAMP_LTZ(TS) as TimeStamp2
FROM DEMO.DEMO.ICYMTA
WHERE EXPECTEDDEPARTURETIME is not null and TRIM(EXPECTEDDEPARTURETIME) != ''
order by EXPECTEDDEPARTURETIME desc;

select EXPECTEDDEPARTURETIME  FROM DEMO.DEMO.ICYMTA;

SHOW USER FUNCTIONS LIKE 'FRESHNESS';
-- LTZ, TZ, DATE


select SNOWFLAKE.CORE.DATA_METRIC_SCHEDULED_TIME()
FROM DEMO.DEMO.ICYMTA;

SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    ORDINAL_POSITION,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    COMMENT
FROM SNOWFLAKE.INFORMATION_SCHEMA.COLUMNS 
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;


select 
EXPECTEDDEPARTURETIME, TO_TIMESTAMP_TZ(EXPECTEDDEPARTURETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM') as TimeStampx , VEHICLEREF, DESTINATIONNAME,TO_TIMESTAMP_LTZ(TS) as TimeStamp2
FROM DEMO.DEMO.ICYMTA
WHERE EXPECTEDDEPARTURETIME is not null and TRIM(EXPECTEDDEPARTURETIME) != ''
order by EXPECTEDDEPARTURETIME desc;

-- Ingestion
select vehiclelocationlatitude, vehiclelocationlongitude, destinationref, recordedattime, stoppointname, bearing, destinationname,
        publishedlinename, arrivalproximitytext, distancefromstop, estimatedpassengercount, ingestion_time, EXPECTEDDEPARTURETIME,
        VEHICLEREF
from icymta
order by ingestion_time desc;

select * from  icymta
order by ingestion_time desc;

CALL DEMO.DEMO.ASK_BUS_DATA('For this month what is the average estimated passenger count');

CALL DEMO.DEMO.RETURN_MTA_NEARBY('40.741260529', '-73.988929749');


    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'snowflake-arctic', 
        CONCAT(
            'You are an expert SQL assistant. Given the table `DEMO.DEMO.ICYMTA` with the following columns: ',
            'TS, PUBLISHEDLINENAME, DESTINATIONNAME, VEHICLEREF, ESTIMATEDPASSENGERCOUNT, ESTIMATEDPASSENGERCAPACITY, PROGRESSSTATUS, VEHICLELOCATIONLATITUDE, VEHICLELOCATIONLONGITUDE. ',
            'Generate a SQL query to answer the following question. Only return the SQL query, with no explanation or other text. Question: ',
            'For this month what is the average estimated passenger count'
        )) FROM ICYMTA LIMIT 1;


 SELECT AVG(ESTIMATEDPASSENGERCOUNT) FROM DEMO.DEMO.ICYMTA WHERE DATE_PART('month', TS) = DATE_PART('month', GETDATE());
        

 SELECT          
      (ST_DISTANCE(ST_MAKEPOINT(VEHICLELOCATIONLATITUDE,VEHICLELOCATIONLONGITUDE),
                   ST_MAKEPOINT(TRY_TO_NUMBER('40.741260529',13,10),TRY_TO_NUMBER('-73.988929749',13,10)))/1609) as distanceinmiles
  FROM demo.demo.icymta
  WHERE DISTANCEFROMSTOP > 0
  AND VEHICLELOCATIONLATITUDE is not null and VEHICLELOCATIONLONGITUDE is not null AND distanceinmiles IS NOT NULL
     LIMIT 10;


SELECT VEHICLEREF as bus, destinationname, 
         expectedarrivaltime,
         EXPECTEDDEPARTURETIME, stoppointname, bearing,
         HAVERSINE( VEHICLELOCATIONLATITUDE, VEHICLELOCATIONLONGITUDE, 
                    TRY_TO_NUMBER('40.741260529',13,10),	
                    TRY_TO_NUMBER('-73.988929749',13,10)  ) as distance, 
         
         (ST_DISTANCE( ST_MAKEPOINT(VEHICLELOCATIONLATITUDE,VEHICLELOCATIONLONGITUDE),
                       ST_MAKEPOINT(TRY_TO_NUMBER('40.741260529',13,10),
                                   TRY_TO_NUMBER('-73.988929749',13,10))) / 1609) as distanceinmiles,                  
         distancefromstop,SITUATIONSIMPLEREF1 as IncidentDescription, recordedattime, ESTIMATEDPASSENGERCOUNT, 
      ESTIMATEDPASSENGERCAPACITY, arrivalproximitytext,NUMBEROFSTOPSAWAY, TS 
  FROM icymta
  WHERE TRY_TO_NUMBER(DISTANCEFROMSTOP,10,1) > 0
  and distance is not null and VEHICLELOCATIONLATITUDE is not null
  and VEHICLELOCATIONLONGITUDE is not null
  ORDER BY distance ASC
     LIMIT 10;
     




   SELECT 
    DATE_TRUNC('HOUR', INGESTION_TIME) AS ingestion_hour,
    COUNT(*) AS total_records,
    
    -- Count empty string coordinates
    COUNT(CASE WHEN VEHICLELOCATIONLATITUDE = '' THEN 1 END) AS empty_latitude_count,
    COUNT(CASE WHEN VEHICLELOCATIONLONGITUDE = '' THEN 1 END) AS empty_longitude_count,
    
    -- Count NULL coordinates  
    COUNT(CASE WHEN VEHICLELOCATIONLATITUDE IS NULL THEN 1 END) AS null_latitude_count,
    COUNT(CASE WHEN VEHICLELOCATIONLONGITUDE IS NULL THEN 1 END) AS null_longitude_count,
    
    -- Count invalid numeric coordinates
    COUNT(CASE 
        WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL 
             AND TRY_TO_DOUBLE(VEHICLELOCATIONLATITUDE) IS NULL 
        THEN 1 
    END) AS invalid_latitude_count,
    
    COUNT(CASE 
        WHEN VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL 
             AND TRY_TO_DOUBLE(VEHICLELOCATIONLONGITUDE) IS NULL 
        THEN 1 
    END) AS invalid_longitude_count,
    
    -- Count valid coordinates
    COUNT(CASE 
        WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL
             AND VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL
             AND TRY_TO_DOUBLE(VEHICLELOCATIONLATITUDE) IS NOT NULL
             AND TRY_TO_DOUBLE(VEHICLELOCATIONLONGITUDE) IS NOT NULL
        THEN 1 
    END) AS valid_coordinate_count,
    
    -- Calculate quality percentages
    ROUND(valid_coordinate_count * 100.0 / total_records, 2) AS valid_coord_percentage,
    ROUND((empty_latitude_count + empty_longitude_count) * 100.0 / (total_records * 2), 2) AS empty_coord_percentage
    
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('DAY', -365, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('HOUR', INGESTION_TIME)
ORDER BY ingestion_hour DESC;


CREATE OR REPLACE TAG NEWYORK_TRANSIT COMMENT = 'New York Transit Data Real-Time';
ALTER ICEBERG TABLE DEMO.DEMO.ICYMTA SET TAG NEWYORK_TRANSIT = 'REALWORLD_DATA';



select 
user_name,
client_application_id
from snowflake.account_usage.sessions
where true
and created_on >= dateadd('days', -90, current_date()) --last 90 days, please adjust
and (
    client_application_id ilike '%SnowSQL%1.0.%'
    or client_application_id ilike '%SnowSQL%1.1.%'
    or client_application_id ilike '%SnowSQL%1.2.%'
)
group by 1,2
order by 1,2;



SELECT * 
FROM demo.demo.icymta 
SAMPLE (25);

SELECT * 
FROM demo.demo.icymta 
SAMPLE BERNOULLI (10) SEED (99);

describe semantic view svmta;


SELECT * FROM SEMANTIC_VIEW(
    svmta
    DIMENSIONS ICYMTA.DESTINATIONNAME
  )
  LIMIT 5;


  CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION mta_anomaly_detector(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'DEMO.DEMO.ICYMTA'),
  SERIES_COLNAME => 'BEARING',
  TIMESTAMP_COLNAME => 'ts',
  TARGET_COLNAME => 'daily_bearing',
  LABEL_COLNAME => 'is_anomaly',
  CONFIG_OBJECT => {
    'detection_method': 'ROBUST_SCALER',
    'contamination': 0.1,
    'standardize_target': true
  }
)
WITH TAG (
  environment = 'production',
  team = 'data_science',
  model_type = 'anomaly_detection'
)
COMMENT = 'Anomaly detection model for identifying unusual daily revenue patterns across pmta';


CREATE SNOWFLAKE.ML.ANOMALY_DETECTION simple_anomaly_model(
  INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'DEMO.DEMO.ICYMTA'),
  TIMESTAMP_COLNAME => 'ts',
  TARGET_COLNAME => 'bearing',
  LABEL_COLNAME => 'DESTINATIONNAME'
)
COMMENT = 'Basic anomaly detection for single time series';