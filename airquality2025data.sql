CREATE ICEBERG TABLE air_quality_data (
    owner STRING,
    city STRING,
    timezone STRING,
    latitude FLOAT,
    periodlabel STRING,
    locality STRING,
    parameterName STRING,
    datetimefrom TIMESTAMP,
    datetimeLast TIMESTAMP,
    uuid STRING,
    name STRING,
    id STRING, 
    state STRING,
    value FLOAT,
    datetimeto TIMESTAMP,
    longitude FLOAT
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'TRANSCOM_TSPANNICEBERG_EXTVOL'
BASE_LOCATION = 'airqualitydata/';

CREATE TABLE air_quality_data_sf (
    owner STRING,
    city STRING,
    timezone STRING,
    latitude FLOAT,
    periodlabel STRING,
    locality STRING,
    parameterName STRING,
    datetimefrom TIMESTAMP,
    datetimeLast TIMESTAMP,
    uuid STRING,
    name STRING,
    id STRING, 
    state STRING,
    value FLOAT,
    datetimeto TIMESTAMP,
    longitude FLOAT
);


select * from aq;


select * from WEATHER_OBSERVATIONS
