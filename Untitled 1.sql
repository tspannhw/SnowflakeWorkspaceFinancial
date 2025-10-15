

-- =============================================================================
-- Stock Instruments API - Package Creation and Deployment
-- =============================================================================
-- Run this script as ACCOUNTADMIN to create and deploy the application package
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- STEP 1: Create Application Package
-- =============================================================================

CREATE APPLICATION PACKAGE IF NOT EXISTS stock_instruments_pkg
    COMMENT = 'Stock Instruments API with Cortex AI-powered news feed';

-- =============================================================================
-- STEP 2: Create Stage for Package Files
-- =============================================================================

CREATE STAGE IF NOT EXISTS stock_instruments_pkg.stage
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Stock Instruments API application files';

-- =============================================================================
-- STEP 3: Upload Files to Stage
-- =============================================================================

-- Upload from local filesystem (adjust paths as needed)
-- PUT file:///path/to/manifest.yml @stock_instruments_pkg.stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file:///path/to/setup.sql @stock_instruments_pkg.stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file:///path/to/README.md @stock_instruments_pkg.stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file:///path/to/streamlit/stock_dashboard.py @stock_instruments_pkg.stage/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Alternative: Copy from existing stage
-- COPY FILES INTO @stock_instruments_pkg.stage 
--   FROM @source_stage
--   PATTERN='.*';

-- =============================================================================
-- STEP 4: Verify Files Uploaded
-- =============================================================================

LIST @DEMO.DEMO.STOCKINSTRUMENTS;

-- Expected files:
-- - manifest.yml
-- - setup.sql
-- - README.md
-- - streamlit/stock_dashboard.py

-- =============================================================================
-- STEP 5: Create Application Version (Using Release Channels)
-- =============================================================================
GRANT USAGE ON INTEGRATION ALPHAVANTAGE_ACCESS TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION ALLOW_ALL_INTEGRATION TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION DEMO_GITHUB_API_INTEGRATION TO ROLE ACCOUNTADMIN;

show integrations;

-- Register the version (new syntax for release channels)
ALTER APPLICATION PACKAGE stock_instruments_pkg
    REGISTER VERSION v1_0_0 
    USING '@DEMO.DEMO.STOCKINSTRUMENTS'
    LABEL = 'Version 1.0.0 - Initial Release with Cortex AI';

-- ALTER APPLICATION PACKAGE my_app_package DEREGISTER VERSION v1;


-- Verify version created
SHOW VERSIONS IN APPLICATION PACKAGE stock_instruments_pkg;

-- =============================================================================
-- STEP 6: Create Release Directive and Set as Default
-- =============================================================================

-- Create a release directive
ALTER APPLICATION PACKAGE stock_instruments_pkg
    ADD RELEASE DIRECTIVE 
    VERSION = v1_0_0
    PATCH = 0;

-- Set the default release directive
ALTER APPLICATION PACKAGE stock_instruments_pkg
    SET DEFAULT RELEASE DIRECTIVE
    VERSION = v1_0_0
    PATCH = 0;

-- Verify release directive
SHOW RELEASE DIRECTIVES IN APPLICATION PACKAGE stock_instruments_pkg;

-- =============================================================================
-- STEP 7: Create Warehouse for Application (if needed)
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS stock_app_wh
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for Stock Instruments API application';

-- =============================================================================
-- STEP 8: Install Application (Development/Testing)
-- =============================================================================

-- Create development instance for testing
-- Note: When using release channels, applications auto-update to latest patch
CREATE APPLICATION IF NOT EXISTS stock_instruments_app_dev
    FROM APPLICATION PACKAGE stock_instruments_pkg
    COMMENT = 'Development instance of Stock Instruments API';

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE stock_app_wh TO APPLICATION stock_instruments_app_dev;

-- =============================================================================
-- STEP 9: Test Development Instance
-- =============================================================================

-- Switch to development app
USE APPLICATION stock_instruments_app_dev;

-- Verify schemas created
SHOW SCHEMAS;

-- Load sample data
CALL core.sp_load_sample_data();

-- Test basic functionality
SELECT * FROM core.vw_latest_prices LIMIT 5;

-- Test Cortex AI sentiment
CALL core.sp_insert_stock_news(
    'AAPL',
    'Test News Article with Positive Sentiment',
    'This is a test article to verify Cortex AI sentiment analysis is working correctly.',
    NULL,
    'Test Source',
    'Test Author',
    CURRENT_TIMESTAMP(),
    'https://test.com/article1'
);

-- Verify sentiment was calculated
SELECT symbol, headline, sentiment_label, sentiment_score
FROM core.stock_news
WHERE headline LIKE 'Test News Article%';

-- Test Cortex Search Service
SHOW CORTEX SEARCH SERVICES IN SCHEMA core;

-- =============================================================================
-- STEP 10: Deploy to Production (if tests pass)
-- =============================================================================

-- Switch back to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Create production instance
-- Applications created from packages with release channels auto-update
CREATE APPLICATION IF NOT EXISTS stock_instruments_app
    FROM APPLICATION PACKAGE stock_instruments_pkg
    COMMENT = 'Production instance of Stock Instruments API';

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE stock_app_wh TO APPLICATION stock_instruments_app;

-- =============================================================================
-- STEP 11: Configure Production Application
-- =============================================================================

USE APPLICATION stock_instruments_app;

-- Load production data (or connect to external APIs)
-- Option 1: Load sample data
CALL core.sp_load_sample_data();

-- Option 2: Configure external API integration
-- See scripts/api_integration.sql for details

-- =============================================================================
-- STEP 12: Grant User Access
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Grant admin role
GRANT APPLICATION ROLE stock_instruments_app.app_admin TO ROLE SYSADMIN;

-- Grant user role
-- GRANT APPLICATION ROLE stock_instruments_app.app_user TO ROLE analyst_role;

-- Grant viewer role
-- GRANT APPLICATION ROLE stock_instruments_app.app_viewer TO ROLE PUBLIC;

-- =============================================================================
-- STEP 13: Configure User Permissions
-- =============================================================================

USE APPLICATION stock_instruments_app;

-- Example: Grant access to specific users
-- CALL security.sp_grant_symbol_access(
--     'user@company.com',
--     ARRAY_CONSTRUCT('AAPL', 'MSFT', 'GOOGL'),
--     100,    -- max API calls per day
--     TRUE    -- can export data
-- );

-- Example: Grant access to all symbols
-- CALL security.sp_grant_symbol_access(
--     'admin@company.com',
--     NULL,   -- NULL = all symbols
--     1000,   -- max API calls per day
--     TRUE    -- can export data
-- );

-- =============================================================================
-- STEP 14: Verify Production Deployment
-- =============================================================================

-- Run comprehensive health check
SELECT 'Application Name' as check_item, CURRENT_DATABASE() as value
UNION ALL
SELECT 'Total Symbols', COUNT(*)::VARCHAR FROM core.stock_symbols WHERE is_active = TRUE
UNION ALL
SELECT 'Total Price Records', COUNT(*)::VARCHAR FROM core.stock_prices
UNION ALL
SELECT 'Total News Articles', COUNT(*)::VARCHAR FROM core.stock_news
UNION ALL
SELECT 'Latest Price Date', MAX(price_date)::VARCHAR FROM core.stock_prices
UNION ALL
SELECT 'Latest News Date', MAX(published_at)::VARCHAR FROM core.stock_news
UNION ALL
SELECT 'Access Log Records', COUNT(*)::VARCHAR FROM security.access_log;

-- Verify Streamlit dashboard
SHOW STREAMLITS IN APPLICATION stock_instruments_app;

-- =============================================================================
-- STEP 15: Enable Monitoring (Optional)
-- =============================================================================

-- Create monitoring schema
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Create health check task
CREATE OR REPLACE TABLE monitoring.health_log (
    check_time TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    active_symbols INTEGER,
    price_records INTEGER,
    news_articles INTEGER,
    latest_price_date DATE,
    recent_access_count INTEGER
);

-- =============================================================================
-- Deployment Complete!
-- =============================================================================

SELECT '✅ Deployment Complete!' as status,
       'Access the application at: Snowsight → Apps → stock_instruments_app' as next_step,
       'Dashboard available at: stock_dashboard' as dashboard;

-- =============================================================================
-- Rollback Plan (if needed)
-- =============================================================================

/*
-- To rollback/remove the application:

USE ROLE ACCOUNTADMIN;

-- Drop production app
DROP APPLICATION IF EXISTS stock_instruments_app;

-- Drop development app
DROP APPLICATION IF EXISTS stock_instruments_app_dev;

-- Drop package (WARNING: This removes all versions)
-- DROP APPLICATION PACKAGE IF EXISTS stock_instruments_pkg;

-- Drop warehouse (if no longer needed)
-- DROP WAREHOUSE IF EXISTS stock_app_wh;
*/

-- =============================================================================
-- Upgrade to New Version (future use with Release Channels)
-- =============================================================================

/*
-- When v1_1_0 is available:

USE ROLE ACCOUNTADMIN;

-- Register new version
ALTER APPLICATION PACKAGE stock_instruments_pkg
    ADD VERSION v1_1_0 
    USING '@stage'
    LABEL = 'Version 1.1.0';

-- Add release directive for new version
ALTER APPLICATION PACKAGE stock_instruments_pkg
    ADD RELEASE DIRECTIVE 
    VERSION = v1_1_0
    PATCH = 0;

-- Set as default (optional - controls new installations)
ALTER APPLICATION PACKAGE stock_instruments_pkg
    SET DEFAULT RELEASE DIRECTIVE
    VERSION = v1_1_0
    PATCH = 0;

-- For existing applications, they will auto-upgrade to latest patch
-- To force upgrade to new version, set release directive on the application:
ALTER APPLICATION stock_instruments_app
    SET RELEASE DIRECTIVE
    VERSION = v1_1_0
    PATCH = 0;

-- Verify version
USE APPLICATION stock_instruments_app;
SELECT SYSTEM$GET_PREDECESSOR_RETURN_VALUE() as current_version;
*/

