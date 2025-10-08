create or replace database FINSERVAM_DEMO;

-- Create a new schema for the sample data to keep it separate
CREATE OR REPLACE SCHEMA FINSERVAM_DEMO.AISQL_DEMO;

-- Set the current database and schema
USE SCHEMA FINSERVAM_DEMO.AISQL_DEMO;


create or replace TABLE FINSERVAM_DEMO.AISQL_DEMO.PM (
	PM VARCHAR(16777216),
	ID NUMBER(19,0)
);

create or replace TABLE FINSERVAM_DEMO.AISQL_DEMO.TRADER (
	TRADER VARCHAR(16777216),
	PM VARCHAR(16777216),
	BUYING_POWER NUMBER(38,0)
);

create or replace TABLE FINSERVAM_DEMO.AISQL_DEMO.TRADE (
	DATE DATE,
	SYMBOL VARCHAR(65535),
	EXCHANGE VARCHAR(1000),
	ACTION VARCHAR(25),
	CLOSE FLOAT,
	NUM_SHARES FLOAT,
	CASH FLOAT,
	TRADER VARCHAR(16777216),
	PM VARCHAR(16777216),
	COST_BASIS FLOAT
);

create or replace TABLE FINSERVAM_DEMO.AISQL_DEMO.STOCK_HISTORY (
	SYMBOL VARCHAR(65535),
	DATE DATE,
	COMPANY VARCHAR(1000),
	EXCHANGE VARCHAR(1000),
	CLOSE FLOAT
);

CREATE OR REPLACE TABLE FINSERVAM_DEMO.AISQL_DEMO.MARKET_NEWS (
    "DATE" DATE,
    NEWS_HEADLINE VARCHAR
);

INSERT INTO FINSERVAM_DEMO.AISQL_DEMO.MARKET_NEWS
VALUES
    ('2023-10-02', 'Apple stock price surges after new product announcement.'),
    ('2023-10-02', 'Google faces regulatory scrutiny, shares drop sharply.'),
    ('2023-10-03', 'Microsoft acquires major AI startup, outlook positive.'),
    ('2023-10-03', 'Tesla misses production targets, stock under pressure.'),
    ('2023-10-04', 'Apple analyst warns of overvaluation; stock price retreats.'),
    ('2023-10-04', 'Alphabet announces stock buyback program, boosts confidence.'),
    ('2023-10-05', 'Amazon reports record e-commerce sales, stock climbs.'),
    ('2023-10-05', 'Microsoft faces lawsuit over cloud computing practices.'),
    ('2023-10-06', 'Netflix loses subscribers in key markets, stock plummets.'),
    ('2023-10-06', 'Google shares rally on new chip launch.');

CREATE OR REPLACE STAGE FINANCIAL_DOCS 
DIRECTORY = ( ENABLE = true ) 
ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

