CREATE OR REPLACE CATALOG INTEGRATION external_iceberg_catalog_int 
  CATALOG_SOURCE = POLARIS 
  TABLE_FORMAT = ICEBERG 
  CATALOG_NAMESPACE = 'ICEBERG_DEMO.ICEBERG_SNOWFLAKE'
  REST_CONFIG = (
    CATALOG_URI = 'https://sfsenorthamerica-tspann_org_aws1.snowflakecomputing.com/polaris/api/catalog' 
    CATALOG_NAME = 'SNOWFLAKEEXTERNAL'
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH 
    OAUTH_CLIENT_ID = 'tspannengineerid' 
    OAUTH_CLIENT_SECRET = 'tspannengineersecret' 
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL') 
  ) 
  ENABLED = TRUE;

describe catalog integration external_iceberg_catalog_int;

  SELECT SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG('external_iceberg_catalog_int', '', 0);

  
  CREATE OR REPLACE ICEBERG TABLE externalsupplier
  CATALOG = 'external_iceberg_catalog_int'
  EXTERNAL_VOLUME = 'TRANSCOM_TSPANNICEBERG_EXTVOL'
  CATALOG_TABLE_NAME = 'SUPPLIER';

--"namespace":"ICEBERG_DEMO.ICEBERG_SNOWFLAKE",
--"name":"SUPPLIER"

SELECT * FROM externalsupplier;

describe table externalsupplier;

SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();

