show iceberg tables;


select CURRENT_REGION();
-- PUBLIC.AWS_US_EAST_1

select * from demo.demo.icymta;

delete from  demo.demo.icymta;

describe iceberg table icymta;


   select VEHICLEREF as bus, destinationname, expectedarrivaltime, EXPECTEDDEPARTURETIME, stoppointname, bearing,HAVERSINE( TO_DECIMAL(VEHICLELOCATIONLATITUDE), TO_DECIMAL(VEHICLELOCATIONLONGITUDE), 40.3209,	-74.4208 ) as distance, 
      distancefromstop,SITUATIONSIMPLEREF1 as IncidentDescription, recordedattime, ESTIMATEDPASSENGERCOUNT, ESTIMATEDPASSENGERCAPACITY, arrivalproximitytext,NUMBEROFSTOPSAWAY, TS 
  from demo.demo.icymta
  where DISTANCEFROMSTOP > 0
  order by distance desc, recordedattime desc ;

  



-- https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-iceberg 

USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT  SET ENABLE_ICEBERG_STREAMING = TRUE;


SELECT 
    DATE_TRUNC('HOUR', INGESTION_TIME) AS ingestion_hour,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN VEHICLELOCATIONLATITUDE IS NULL THEN 1 END) AS missing_latitude,
    COUNT(CASE WHEN VEHICLELOCATIONLONGITUDE IS NULL THEN 1 END) AS missing_longitude,
    COUNT(CASE WHEN VEHICLEREF IS NULL THEN 1 END) AS missing_vehicle_ref,
    COUNT(CASE WHEN LINEREF IS NULL THEN 1 END) AS missing_line_ref,
    (missing_latitude + missing_longitude + missing_vehicle_ref + missing_line_ref) * 100.0 / total_records AS quality_score_pct
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('HOUR', INGESTION_TIME)
ORDER BY ingestion_hour DESC;

alter iceberg table icymta 
   ADD COLUMN INGESTION_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
   -- 

alter iceberg table icymta 
   ADD COLUMN  SOURCE_SYSTEM STRING DEFAULT 'MTA_REALTIME';

alter iceberg table icymta 
   ADD COLUMN 
    PROCESSING_STATUS STRING DEFAULT 'PROCESSED';

select * from MTABUSVEHICLEMONITORING;


create or replace ICEBERG TABLE DEMO.DEMO.ICYMTA (
	STOPPOINTREF STRING,
	VEHICLEREF STRING,
	PROGRESSRATE STRING,
	EXPECTEDDEPARTURETIME STRING,
	STOPPOINT STRING,
	VISITNUMBER STRING,
	DATAFRAMEREF STRING,
	STOPPOINTNAME STRING,
	SITUATIONSIMPLEREF5 STRING,
	SITUATIONSIMPLEREF3 STRING,
	BEARING STRING,
	SITUATIONSIMPLEREF4 STRING,
	SITUATIONSIMPLEREF1 STRING,
	ORIGINAIMEDDEPARTURETIME STRING,
	SITUATIONSIMPLEREF2 STRING,
	JOURNEYPATTERNREF STRING,
	RECORDEDATTIME STRING,
	OPERATORREF STRING,
	DESTINATIONNAME STRING,
	EXPECTEDARRIVALTIME STRING,
	BLOCKREF STRING,
	LINEREF STRING,
	VEHICLELOCATIONLONGITUDE STRING WITH TAG (SNOWFLAKE.CORE.PRIVACY_CATEGORY='QUASI_IDENTIFIER', SNOWFLAKE.CORE.SEMANTIC_CATEGORY='LONGITUDE'),
	DIRECTIONREF STRING,
	ARRIVALPROXIMITYTEXT STRING,
	DISTANCEFROMSTOP STRING,
	ESTIMATEDPASSENGERCAPACITY STRING,
	AIMEDARRIVALTIME STRING,
	PUBLISHEDLINENAME STRING,
	DATEDVEHICLEJOURNEYREF STRING,
	DATE STRING,
	MONITORED STRING,
	PROGRESSSTATUS STRING,
	DESTINATIONREF STRING,
	ESTIMATEDPASSENGERCOUNT STRING,
	VEHICLELOCATIONLATITUDE STRING WITH TAG (SNOWFLAKE.CORE.PRIVACY_CATEGORY='QUASI_IDENTIFIER', SNOWFLAKE.CORE.SEMANTIC_CATEGORY='LATITUDE'),
	ORIGINREF STRING,
	NUMBEROFSTOPSAWAY STRING,
	TS STRING,
	UUID STRING,
    INGESTION_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP(6),
    SOURCE_SYSTEM STRING DEFAULT 'MTA_REALTIME',
    PROCESSING_STATUS STRING DEFAULT 'PROCESSED'
)
 EXTERNAL_VOLUME = 'TRANSCOM_TSPANNICEBERG_EXTVOL'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'mta/';


select * from demo.demo.icymta;
-- do on insert CURRENT_TIMESTAMP()


select * from MTABUSVEHICLEMONITORING;



CREATE CORTEX AGENT mtabusassistant
INTEGRATIONS = [
    'CORTEX_ANALYST:demo.demo.MTAICEBERGSEMANTICVIEW'
]
SYSTEM_MESSAGE = 'You are a helpful data analyst assistant that can search mta bus data and analyze data using SQL.'
DESCRIPTION = 'An AI assistant for data analysis of mta buses'
INSTRUCTIONS = 'Always provide clear explanations with your analysis and cite sources when using search results.';


 CREATE OR REPLACE STREAM ICYMTA_STREAM ON TABLE DEMO.DEMO.ICYMTA;

 CREATE OR REPLACE VIEW V_STREAMING_METRICS AS
SELECT 
    DATE_TRUNC('MINUTE', INGESTION_TIME) AS ingestion_minute,
    COUNT(*) AS records_per_minute,
    COUNT(DISTINCT VEHICLEREF) AS unique_vehicles,
    COUNT(DISTINCT LINEREF) AS unique_lines,
    AVG(DISTANCEFROMSTOP) AS avg_distance_from_stop,
    MAX(INGESTION_TIME) AS latest_ingestion
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('MINUTE', INGESTION_TIME)
ORDER BY ingestion_minute DESC;


select * from V_STREAMING_METRICS;

-- Data quality monitoring view
CREATE OR REPLACE VIEW V_DATA_QUALITY_METRICS AS
SELECT 
    DATE_TRUNC('HOUR', INGESTION_TIME) AS ingestion_hour,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN VEHICLELOCATIONLATITUDE IS NULL THEN 1 END) AS missing_latitude,
    COUNT(CASE WHEN VEHICLELOCATIONLONGITUDE IS NULL THEN 1 END) AS missing_longitude,
    COUNT(CASE WHEN VEHICLEREF IS NULL THEN 1 END) AS missing_vehicle_ref,
    COUNT(CASE WHEN LINEREF IS NULL THEN 1 END) AS missing_line_ref,
    (missing_latitude + missing_longitude + missing_vehicle_ref + missing_line_ref) * 100.0 / total_records AS quality_score_pct
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('HOUR', INGESTION_TIME)
ORDER BY ingestion_hour DESC;

select * from V_DATA_QUALITY_METRICS;

GRANT ALL ON TABLE DEMO.DEMO.ICYMTA TO ROLE PUBLIC;
GRANT ALL ON STREAM ICYMTA_STREAM TO ROLE PUBLIC;
GRANT SELECT ON VIEW V_STREAMING_METRICS TO ROLE PUBLIC;
GRANT SELECT ON VIEW V_DATA_QUALITY_METRICS TO ROLE PUBLIC;

-- Step 1: Connect as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Step 2: Enable Iceberg tables at account level
ALTER ACCOUNT SET ENABLE_ICEBERG_TABLES = TRUE;


ALTER account dr75630 SET ENABLE_ICEBERG_EXTERNAL_TABLES=TRUE, parameter_comment='SNOW-579103 Enable Iceberg external tables for PrPr';


  USE ROLE ACCOUNTADMIN;

     DESC USER kafkaguy;

     ALTER ACCOUNT kafkaguy SET ENABLE_ICEBERG_STREAMING = true;
     

SHOW PARAMETERS LIKE 'ENABLE_ICEBERG_STREAMING' IN ACCOUNT;

ALTER ACCOUNT SET ENABLE_ICEBERG_STREAMING = true;


-- https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-iceberg

ALTER ACCOUNT SET ENABLE_ICEBERG_STREAMING = TRUE;


ALTER ACCOUNT SET ENABLE_ICEBERG_STREAMING_INGEST_DEFRAG = TRUE;


SHOW PARAMETERS LIKE 'E%' IN ACCOUNT;






-- ============================================================================
-- FIX HAVERSINE FUNCTION ERRORS IN SNOWFLAKE VIEWS
-- Problem: "Numeric value '' is not recognized" 
-- Solution: Properly handle empty strings and convert to numeric values
-- ============================================================================

-- 1. Fix V_STREAMING_METRICS with proper coordinate handling
CREATE OR REPLACE VIEW V_STREAMING_METRICS AS
SELECT 
    DATE_TRUNC('MINUTE', INGESTION_TIME) AS ingestion_minute,
    COUNT(*) AS records_per_minute,
    COUNT(DISTINCT VEHICLEREF) AS unique_vehicles,
    COUNT(DISTINCT LINEREF) AS unique_lines,
    
    -- Handle numeric conversions with proper NULL handling
    AVG(
        CASE 
            WHEN DISTANCEFROMSTOP = '' OR DISTANCEFROMSTOP IS NULL THEN NULL
            ELSE TRY_CAST(DISTANCEFROMSTOP AS DOUBLE)
        END
    ) AS avg_distance_from_stop,
    
    -- Count valid coordinates (non-empty, numeric)
    COUNT(
        CASE 
            WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL 
                 AND VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL
                 AND TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE) IS NOT NULL
                 AND TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE) IS NOT NULL
            THEN 1 
        END
    ) AS vehicles_with_valid_coords,
    
    -- Average coordinates (only for valid values)
    AVG(
        CASE 
            WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL
            THEN TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE)
        END
    ) AS avg_latitude,
    
    AVG(
        CASE 
            WHEN VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL
            THEN TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE)
        END
    ) AS avg_longitude,
    
    MAX(INGESTION_TIME) AS latest_ingestion
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('MINUTE', INGESTION_TIME)
ORDER BY ingestion_minute DESC;

-- 2. Create a fixed VW_MTANEARBY view with proper Haversine function
CREATE OR REPLACE VIEW VW_MTANEARBY AS
WITH valid_coordinates AS (
    SELECT 
        *,
        -- Convert string coordinates to numeric, handling empty strings
        CASE 
            WHEN VEHICLELOCATIONLATITUDE = '' OR VEHICLELOCATIONLATITUDE IS NULL THEN NULL
            ELSE TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE)
        END AS lat_numeric,
        CASE 
            WHEN VEHICLELOCATIONLONGITUDE = '' OR VEHICLELOCATIONLONGITUDE IS NULL THEN NULL
            ELSE TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE)
        END AS lon_numeric
    FROM DEMO.DEMO.ICYMTA
    WHERE INGESTION_TIME >= DATEADD('HOUR', -2, CURRENT_TIMESTAMP())
),
nearby_calculations AS (
    SELECT 
        a.*,
        b.VEHICLEREF AS nearby_vehicle,
        b.LINEREF AS nearby_line,
        b.lat_numeric AS nearby_lat,
        b.lon_numeric AS nearby_lon,
        
        -- Safe Haversine calculation (only for valid coordinates)
        CASE 
            WHEN a.lat_numeric IS NOT NULL AND a.lon_numeric IS NOT NULL 
                 AND b.lat_numeric IS NOT NULL AND b.lon_numeric IS NOT NULL
                 AND a.VEHICLEREF != b.VEHICLEREF  -- Don't compare vehicle to itself
            THEN HAVERSINE(a.lat_numeric, a.lon_numeric, b.lat_numeric, b.lon_numeric)
            ELSE NULL
        END AS distance_km
    FROM valid_coordinates a
    CROSS JOIN valid_coordinates b
    WHERE a.lat_numeric IS NOT NULL 
      AND a.lon_numeric IS NOT NULL
      AND b.lat_numeric IS NOT NULL 
      AND b.lon_numeric IS NOT NULL
      AND a.VEHICLEREF != b.VEHICLEREF
)
SELECT 
    VEHICLEREF,
    LINEREF,
    STOPPOINTREF,
    PUBLISHEDLINENAME,
    lat_numeric AS vehicle_latitude,
    lon_numeric AS vehicle_longitude,
    RECORDEDATTIME,
    nearby_vehicle,
    nearby_line,
    nearby_lat,
    nearby_lon,
    distance_km,
    RANK() OVER (PARTITION BY VEHICLEREF ORDER BY distance_km ASC) AS proximity_rank
FROM nearby_calculations
WHERE distance_km IS NOT NULL 
  AND distance_km <= 5.0  -- Within 5km
ORDER BY VEHICLEREF, distance_km;

-- 3. Create a data quality view to identify coordinate issues
CREATE OR REPLACE VIEW V_COORDINATE_QUALITY AS
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
             AND TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE) IS NULL 
        THEN 1 
    END) AS invalid_latitude_count,
    
    COUNT(CASE 
        WHEN VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL 
             AND TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE) IS NULL 
        THEN 1 
    END) AS invalid_longitude_count,
    
    -- Count valid coordinates
    COUNT(CASE 
        WHEN VEHICLELOCATIONLATITUDE != '' AND VEHICLELOCATIONLATITUDE IS NOT NULL
             AND VEHICLELOCATIONLONGITUDE != '' AND VEHICLELOCATIONLONGITUDE IS NOT NULL
             AND TRY_CAST(VEHICLELOCATIONLATITUDE AS DOUBLE) IS NOT NULL
             AND TRY_CAST(VEHICLELOCATIONLONGITUDE AS DOUBLE) IS NOT NULL
        THEN 1 
    END) AS valid_coordinate_count,
    
    -- Calculate quality percentages
    ROUND(valid_coordinate_count * 100.0 / total_records, 2) AS valid_coord_percentage,
    ROUND((empty_latitude_count + empty_longitude_count) * 100.0 / (total_records * 2), 2) AS empty_coord_percentage
    
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('DAY', -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('HOUR', INGESTION_TIME)
ORDER BY ingestion_hour DESC;

-- 4. Create a utility function to safely convert coordinates
CREATE OR REPLACE FUNCTION SAFE_COORDINATE_CAST(coord_string STRING)
RETURNS DOUBLE
LANGUAGE SQL
AS
$$
    CASE 
        WHEN coord_string = '' OR coord_string IS NULL THEN NULL
        ELSE TRY_CAST(coord_string AS DOUBLE)
    END
$$;

-- 5. Create a helper view with clean coordinates for easy querying
CREATE OR REPLACE VIEW V_CLEAN_COORDINATES AS
SELECT 
    *,
    SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE) AS clean_latitude,
    SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE) AS clean_longitude,
    
    -- Validate coordinate ranges (NYC area: Lat ~40.4-40.9, Lon ~-74.3 to -73.7)
    CASE 
        WHEN SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE) BETWEEN 40.0 AND 41.0
             AND SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE) BETWEEN -75.0 AND -73.0
        THEN TRUE
        ELSE FALSE
    END AS coordinates_valid_range,
    
    -- Distance from Manhattan center (roughly 40.7589, -73.9851)
    CASE 
        WHEN SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE) IS NOT NULL 
             AND SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE) IS NOT NULL
        THEN HAVERSINE(
            SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE), 
            SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE),
            40.7589, 
            -73.9851
        )
        ELSE NULL
    END AS distance_from_manhattan_km
    
FROM DEMO.DEMO.ICYMTA
WHERE INGESTION_TIME >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP());

-- ============================================================================
-- GRANTS
-- ============================================================================

GRANT SELECT ON VIEW V_STREAMING_METRICS TO ROLE PUBLIC;
GRANT SELECT ON VIEW VW_MTANEARBY TO ROLE PUBLIC;
GRANT SELECT ON VIEW V_COORDINATE_QUALITY TO ROLE PUBLIC;
GRANT SELECT ON VIEW V_CLEAN_COORDINATES TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION SAFE_COORDINATE_CAST(STRING) TO ROLE PUBLIC;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

select * from demo.demo.icymta order by ingestion_time desc  limit 50;

-- Check coordinate data quality
SELECT * FROM V_COORDINATE_QUALITY LIMIT 10;

-- Test the fixed views
SELECT * FROM V_STREAMING_METRICS LIMIT 5;

select * from demo.demo.icymta;

SELECT * FROM VW_MTANEARBY LIMIT 5;

-- Show sample coordinate conversions
SELECT 
    VEHICLEREF,
    VEHICLELOCATIONLATITUDE AS original_lat,
    VEHICLELOCATIONLONGITUDE AS original_lon,
    SAFE_COORDINATE_CAST(VEHICLELOCATIONLATITUDE) AS clean_lat,
    SAFE_COORDINATE_CAST(VEHICLELOCATIONLONGITUDE) AS clean_lon,
    coordinates_valid_range
FROM V_CLEAN_COORDINATES 
WHERE VEHICLELOCATIONLATITUDE != '' 
LIMIT 10;