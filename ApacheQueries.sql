SELECT * FROM
snowflake.account_usage.table_dml_history
WHERE start_time > DATEADD(hour, -128,
CURRENT_TIMESTAMP());



select * from SENSORS;

select * from flight_data_iceberg;

select * from planes;

select * from AQ;

select * from AQFORECAST;

Select * from ICYMTA order by INGESTION_TIME desc;

select * from TRAVELADVISORIES;

select * from  SUBWAY
order by tripstartdate desc, departuretime desc;

select * from VEHICLEPOSITIONS
order by STARTDATE desc, TIMESTAMP desc;

 select * from noaaweather;

 select * from planes;


-- flights, delays, the weather, air quality, local sensors, travel advisories, reviews, social media, 
-- local transit 

 -- hyatt regency 44.9705° N, 93.2781° W

 
 --

 
select flight, planeid, concat(latitude, ',', longitude) as latlong,
        nic, squawk, tisb, altgeom, altbaro, 
(ST_DISTANCE(ST_MAKEPOINT(longitude,latitude), 
             ST_MAKEPOINT(-93.2781,44.9705 ))/1609) 
             as distanceinmiles,
             TO_CHAR(TO_TIMESTAMP(ts), 'YYYY-MM-DD HH24:MI:SS') as currTime
from planes
where latitude is not null and longitude is not null
and trim(latitude) != '' and trim(longitude) != '' and 
distanceinmiles <= 100
  ORDER BY distanceinmiles ASC, ts desc 
     LIMIT 250;



 ------

 CREATE TAG data_sensitivity
  PROPAGATE = ON_DEPENDENCY_AND_DATA_MOVEMENT
  ON_CONFLICT = 'HIGHLY CONFIDENTIAL';
  
 -- call DEMO.DEMO.RETURN_MTA_NEARBY('44.9705', '-93.2781');
 
-- DEMO.DEMO.WEATHER definition

create or replace ICEBERG TABLE DEMO.DEMO.NOAAWEATHER(
	CREDIT STRING,
	CREDIT_URL STRING,
	IMAGE_URL STRING,
	IMAGE_TITLE STRING,
	IMAGE_LINK STRING,
	STATION_ID STRING,
	LOCATION STRING,
	LATITUDE FLOAT,
	LONGITUDE FLOAT,
	SUGGESTED_PICKUP STRING,
	SUGGESTED_PICKUP_PERIOD FLOAT,
	OBSERVATION_TIME STRING,
	OBSERVATION_TIME_RFC822 STRING,
	WEATHER STRING, 
	TEMPERATURE_STRING STRING,
	TEMP_F FLOAT,
	TEMP_C FLOAT,
	RELATIVE_HUMIDITY FLOAT,
	WIND_STRING STRING,
	WIND_DIR STRING,
	WIND_DEGREES FLOAT,
	WIND_MPH FLOAT,
	WIND_KT  FLOAT,
	PRESSURE_IN FLOAT,
	DEWPOINT_STRING STRING,
	DEWPOINT_F FLOAT,
	DEWPOINT_C FLOAT,
	VISIBILITY_MI FLOAT,
	ICON_URL_BASE STRING,
	TWO_DAY_HISTORY_URL STRING,
	ICON_URL_NAME STRING,
	OB_URL STRING,
	DISCLAIMER_URL STRING,
	COPYRIGHT_URL STRING,
	PRIVACY_POLICY_URL STRING,
	CREATED_AT TIMESTAMP_NTZ,
	UPDATED_AT TIMESTAMP_NTZ
) EXTERNAL_VOLUME = 'TRANSCOM_TSPANNICEBERG_EXTVOL'
 CATALOG = 'SNOWFLAKE'
 BASE_LOCATION = 'noaaweather/';


 select * from noaaweather;
 