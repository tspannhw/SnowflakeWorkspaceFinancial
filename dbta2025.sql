call RETURN_MTA_NEARBY('40.3209','-74.4208');

-- Hyatt Regency Boston
-- @42.3533183,-71.0614467,

  --- multimodal
  -- https://docs.snowflake.com/en/user-guide/snowflake-cortex/complete-multimodal#process-images
-- pixtral-large
-- claude-3-5-sonnet
  SELECT SNOWFLAKE.CORTEX.COMPLETE('pixtral-large',
    'Fully describe the image in at least 100 words',
    TO_FILE('@images', 'IMG_3116.jpg'));

    SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY;


SELECT
    user_name,
    request_id,
    response_status_code,
    semantic_model_type,
    latest_question AS input,
    response_body AS output,
    feedback[ARRAY_SIZE(feedback) - 1] as feedback,
    TO_CHAR(timestamp, 'Mon DD, YYYY, HH12:MI:SS AM') AS formatted_timestamp
FROM table(snowflake.local.CORTEX_ANALYST_REQUESTS('SEMANTIC_VIEW', 'DEMO.DEMO.WEATHER'))
 ORDER BY timestamp DESC LIMIT 30 OFFSET 0;

 
    
alter stage DOCUMENTS refresh;

CREATE OR REPLACE PROCEDURE DEMO.DEMO.PARSEDOCS(FILENAME STRING)
RETURNS TABLE ("PARSEDDOC" VARCHAR )
LANGUAGE SQL
EXECUTE AS OWNER
AS 
DECLARE
  res RESULTSET;
BEGIN
  ALTER STAGE DOCUMENTS REFRESH; 
  res := ( SELECT TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@DOCUMENTS, :FILENAME)) as PARSEDDOC );
  RETURN TABLE(res);
END;

CREATE OR REPLACE TABLE PARSEDCONTENT (
	content_id NUMBER(38,0) primary key autoincrement start 1 increment 1 noorder,
	content VARCHAR(16777216),
    metadata VARCHAR(16777216)
);

desc table PARSEDCONTENT;

select * from PARSEDCONTENT;


select * FROM PLANES;

select * FROM AQ;

-- https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/tutorials/cortex-search-tutorial-1-search#step-3-create-the-search-service

CREATE OR REPLACE WAREHOUSE CORTEX_SEARCH_AQ_WH
WITH
     WAREHOUSE_SIZE='X-SMALL'
     AUTO_SUSPEND = 120
     AUTO_RESUME = TRUE
     INITIALLY_SUSPENDED=TRUE;
     

SELECT SYSTEM$ENABLE_BEHAVIOR_CHANGE_BUNDLE('2025_03');

     
CREATE OR REPLACE CORTEX SEARCH SERVICE DEMO.DEMO.AIRQUALITY_SRVC
ON AIRQUALITY_TEXT
ATTRIBUTES REPORTINGAREA, STATECODE, LATITUDE, LONGITUDE, DATEOBSERVED, PARAMETERNAME, AQI, DOC_TITLE, DOC_ID
WAREHOUSE = CORTEX_SEARCH_AQ_WH
TARGET_LAG = '1 hour'
AS
        SELECT UUID as DOC_ID, 
               (REPORTINGAREA || ' ' || PARAMETERNAME || '=' || AQI || ' @ ' || DATEOBSERVED || ' ' || HOUROBSERVED  ) as DOC_TITLE, 
               DATEOBSERVED,HOUROBSERVED,
               REPORTINGAREA, STATECODE, 
               LATITUDE, LONGITUDE,
               PARAMETERNAME,AQI,
        ('Air Quality Report for ' || REPORTINGAREA || ' ' || STATECODE  || ' of ' ||  PARAMETERNAME || ' is ' || AQI || ' which is ' || CATEGORYNAME || '.  Observation date/time was ' || DATEOBSERVED || ' at the hour of ' || HOUROBSERVED  || '.\n\n' ) as AIRQUALITY_TEXT
    FROM DEMO.DEMO.AQ;

    


    select *  FROM DEMO.DEMO.AQ;

    DESC CORTEX SEARCH SERVICE DEMO.DEMO.AIRQUALITY_SRVC;
    GRANT USAGE ON CORTEX SEARCH SERVICE DEMO.DEMO.AIRQUALITY_SRVC TO ROLE ACCOUNTADMIN;

    -- POST https://<account_identifier>.snowflakecomputing.com/api/v2/cortex/inference:complete

    -- https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-llm-rest-api

    -- https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/query-cortex-search-service


    
SELECT *
FROM
  TABLE (
    CORTEX_SEARCH_DATA_SCAN (
      SERVICE_NAME => 'AIRQUALITY_SRVC'
    )
  ) LIMIT 5 ;

  SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'DEMO.DEMO.AIRQUALITY_SRVC',
      '{
        "query": "PM10 is Good",
        "columns":[
            "AIRQUALITY_TEXT",
            "REPORTINGAREA",
            "STATECODE",
            "DATEOBSERVED",
            "HOUROBSERVED",
            "PARAMETERNAME",
            "AQI"
        ],
        "filter": {"@eq": {"REPORTINGAREA": "Boston"} },
        "limit":10
      }'
  )
)['results'] as results;

    
        SELECT DATEOBSERVED,HOUROBSERVED,REPORTINGAREA, STATECODE, LATITUDE, LONGITUDE,
        ('Air Quality Report for ' || REPORTINGAREA || ' ' || STATECODE  || ' of ' ||  PARAMETERNAME || ' is ' || AQI || ' which is ' || CATEGORYNAME || '.  Observation date/time was ' || DATEOBSERVED || ' at the hour of ' || HOUROBSERVED  || '.\n\n' ) as AIRQUALITY_TEXT
    FROM DEMO.DEMO.AQ where STATECODE = 'MA';

    select * FROM DEMO.DEMO.AQ where STATECODE = 'MA';
    
CALL DEMO.DEMO.PARSEDOCS('14May2025_From%20Air%20Quality%20to%20Aircraft%20%26%20Automobiles%2C%20Unstructured%20Data%20Is%20Everywhere.pdf.gz');

SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@DOCUMENTS, 'streamingai.pdf.gz', {'mode': 'LAYOUT'});

SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@DOCUMENTS, '${pdffile}', {'mode': 'LAYOUT'});

list @DOCUMENTS;
SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@DOCUMENTS, '2024dec-pydataglobal-tutorial-itsintheairtonight-241205133450-e10d8e91.pdf.gz');

SELECT TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@DOCUMENTS, 'streamingai.pdf.gz')) AS OCR;

  SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@DOCUMENTS, 'tspann-2024-nov-cloudx-addinggenerativeaitoreal-timestreamingpipelines-241114203020-192327c8.pdf', {'mode': 'LAYOUT'});


  SELECT SNOWFLAKE.CORTEX.COMPLETE('pixtral-large',
    'Fully describe the image in at least 100 words',
    TO_FILE('@images', 'IMG_3116.jpg'));
    

    SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
      'DEMO.DEMO.AIRQUALITY_SRVC',
      '{
        "query": "PM10 is Good",
        "columns":[
            "AIRQUALITY_TEXT",
            "REPORTINGAREA",
            "STATECODE",
            "DATEOBSERVED",
            "HOUROBSERVED",
            "PARAMETERNAME",
            "AQI"
        ],
        "filter": {"@eq": {"REPORTINGAREA": "Boston"} },
        "limit":10
      }'
  )
)['results'] as results;


  SELECT SNOWFLAKE.CORTEX.COMPLETE( 'snowflake-llama-3.3-70b', 'You are an expert air quality assistant that extracs information from the CONTEXT provided between <context> and </context> tags.
When ansering the question contained between <question> and </question> tags
be concise and do not hallucinate. If you don´t have the information just say so. Only anwer the question if you can extract it from the CONTEXT provideed. Do not mention the CONTEXT used in your answer.
<context>Air Quality Report for Boston MA of O3 is 17 which is Good.  Observation date/time was 2025-05-14 at the hour of 7.</context><question>What is the air quality like right now in Boston?</question>Answer:' ) as aqchat;


  select * from demo.demo.aq where statecode = 'MA' and reportingarea ='Boston' order by dateobserved desc;

  