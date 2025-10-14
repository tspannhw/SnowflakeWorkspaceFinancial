-- ================================================================================
-- Semantic View Auto-Generation Stored Procedure
-- 
-- Description: Automatically generates semantic views from specified tables
-- Created: 20251009
-- Created by Tsubasa Kanno @Snowflake
-- ================================================================================

CREATE OR REPLACE PROCEDURE GENERATE_SEMANTIC_VIEW(
    SOURCE_DATABASE VARCHAR,      -- Source database name
    SOURCE_SCHEMA VARCHAR,        -- Source schema name
    SOURCE_TABLE VARCHAR,         -- Source table name
    TARGET_DATABASE VARCHAR,      -- Target database name
    TARGET_SCHEMA VARCHAR,        -- Target schema name
    TARGET_VIEW_NAME VARCHAR,     -- Semantic view name to create
    LLM_MODEL VARCHAR DEFAULT 'claude-sonnet-4-5' -- LLM model to use
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    -- Table information
    column_info VARCHAR;
    sample_data_json VARCHAR;
    table_comment VARCHAR;

    -- AI generation results
    ai_response VARCHAR;

    -- Final SQL
    create_view_sql VARCHAR;

BEGIN
    -- ================================================================================
    -- STEP 1: Retrieve table information
    -- ================================================================================

    -- Get column information (using DESCRIBE TABLE)
    DECLARE
        describe_sql VARCHAR;
        rs RESULTSET;
    BEGIN
        describe_sql := 'DESCRIBE TABLE ' || :SOURCE_DATABASE || '.' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE;
        rs := (EXECUTE IMMEDIATE :describe_sql);

        -- Get information from results
        -- $1: Column name, $2: Data type, $10: Comment
        SELECT LISTAGG(
            $1 || ':' || $2 || CASE WHEN $10 IS NOT NULL AND $10 != '' THEN '(' || $10 || ')' ELSE '' END,
            ', '
        ) INTO column_info
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    EXCEPTION
        WHEN OTHER THEN
            RETURN 'Error: Could not retrieve table information - ' || :SOURCE_DATABASE || '.' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE || '. Error: ' || SQLERRM;
    END;

    -- Error check
    IF (column_info IS NULL OR LENGTH(:column_info) = 0) THEN
        RETURN 'Error: Column information is empty - ' || :SOURCE_DATABASE || '.' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE;
    END IF;

    -- Get table comment
    SELECT COMMENT INTO table_comment
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = :SOURCE_DATABASE
      AND TABLE_SCHEMA = :SOURCE_SCHEMA
      AND TABLE_NAME = :SOURCE_TABLE;

    -- Get sample data (first 5 rows)
    DECLARE
        sample_sql VARCHAR;
        sample_rs RESULTSET;
    BEGIN
        sample_sql := 'SELECT * FROM ' || :SOURCE_DATABASE || '.' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE || ' LIMIT 5';
        sample_rs := (EXECUTE IMMEDIATE :sample_sql);

        -- Get sample data in JSON format
        SELECT TO_VARCHAR(ARRAY_AGG(OBJECT_CONSTRUCT(*))) INTO sample_data_json
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    EXCEPTION
        WHEN OTHER THEN
            sample_data_json := 'Sample data unavailable';
    END;

    -- ================================================================================
    -- STEP 2: Generate complete Semantic View definition with AI
    -- ================================================================================

    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        :LLM_MODEL,
        CONCAT(
            '[IMPORTANT] Generate a Snowflake semantic view using the information below.',
            '\n\n★ DO NOT use any column names that do not exist ★',
            '\n\n[TABLE INFORMATION]',
            '\nTable: ', :SOURCE_DATABASE, '.', :SOURCE_SCHEMA, '.', :SOURCE_TABLE,
            '\nAlias: ', :SOURCE_TABLE,
            '\nTable Comment: ', COALESCE(:table_comment, 'None'),
            '\n\n[EXISTING COLUMNS (Use ONLY these)]',
            '\n', :column_info,
            '\n\n[SAMPLE DATA (For understanding data usage)]',
            '\n', SUBSTRING(:sample_data_json, 1, 1000),
            '\n\n[SQL TO GENERATE]',
            '\nCREATE OR REPLACE SEMANTIC VIEW ', :TARGET_DATABASE, '.', :TARGET_SCHEMA, '.', :TARGET_VIEW_NAME,
            '\n  TABLES (',
            '\n    ', :SOURCE_TABLE, ' AS ', :SOURCE_DATABASE, '.', :SOURCE_SCHEMA, '.', :SOURCE_TABLE,
            '\n      WITH SYNONYMS (''alias1'', ''alias2'', ''alias3'', ''alias4'')',
            '\n      COMMENT = ''Detailed table description''',
            '\n  )',
            '\n  FACTS (',
            '\n    ', :SOURCE_TABLE, '.fact_name AS existing_column_name',
            '\n      WITH SYNONYMS (''alias1'', ''alias2'')',
            '\n      COMMENT = ''Detailed column description''',
            '\n  )',
            '\n  DIMENSIONS (',
            '\n    ', :SOURCE_TABLE, '.dim_name AS existing_column_name',
            '\n      WITH SYNONYMS (''alias1'', ''alias2'')',
            '\n      COMMENT = ''Detailed column description''',
            '\n  )',
            '\n  METRICS (',
            '\n    ', :SOURCE_TABLE, '.metric_name AS SUM(existing_column_name)',
            '\n      WITH SYNONYMS (''alias1'', ''alias2'')',
            '\n      COMMENT = ''Detailed column description''',
            '\n  )',
            '\n  COMMENT = ''Detailed semantic view description'';',
            '\n\n[ABSOLUTE RULES]',
            '\n★★★ MOST IMPORTANT ★★★ Use ONLY column names from the above list',
            '\n\n0. Omit Primary Key:',
            '\n   - Do not write PRIMARY KEY clause',
            '\n   - Primary key constraints do not function in Snowflake and are optional',
            '\n\n1. Column Name Usage:',
            '\n   - Column names used on the right side of AS must also exist in the list',
            '\n   - Example OK: YEAR(TRANSACTION_DATE) ← only if TRANSACTION_DATE exists',
            '\n   - Example NG: YEAR(ORDER_DATE) ← if ORDER_DATE does not exist',
            '\n\n2. FACTS (Numeric Data):',
            '\n   - Quantity, amount, price, score, rate - numeric type columns',
            '\n   - Include both simple column references and calculated formulas',
            '\n   - Example (simple): QUANTITY, PRICE, AMOUNT',
            '\n   - Example (calculated): revenue AS QUANTITY * UNIT_PRICE, discount_amount AS TOTAL_PRICE * 0.1',
            '\n\n3. DIMENSIONS (Attribute Data):',
            '\n   - IDs, names, dates, categories, statuses',
            '\n   - Do not overlap with FACTS',
            '\n\n4. No Duplicate Names: All table.name combinations on the left must be unique',
            '\n\n5. Date Derivative Naming: original_column_name_year, original_column_name_month',
            '\n\n6. Rich Synonyms (Critical for Cortex Analyst to understand user requests):',
            '\n   - Tables: 4-5 business-relevant aliases',
            '\n   - Each DIMENSION, FACT, METRIC: 2-3 natural aliases',
            '\n   - Infer appropriate synonyms from sample data actual usage',
            '\n   - Example: sales amount → "total sales", "sales total", "revenue"',
            '\n   - Example: product ID → "product code", "item number", "SKU"',
            '\n\n7. Rich COMMENTS:',
            '\n   - Generate specific descriptions from sample data',
            '\n   - Include business meaning',
            '\n   - Specify units and ranges',
            '\n\n8. Utilize Sample Data:',
            '\n   - Understand actual usage from sample data content',
            '\n   - Generate more concrete, practical synonyms',
            '\n   - Reflect data characteristics (range, units, etc.) in COMMENTS',
            '\n\n9. Generate Abundant FACTS and DIMENSIONS:',
            '\n   - FACTS: Around 5-10 (simple columns + calculated formulas)',
            '\n   - DIMENSIONS: Around 5-15 (original columns + date derivatives)',
            '\n   - METRICS: Around 4-8 (various aggregation patterns)',
            '\n\n10. Output: SQL syntax only (no ```, no explanatory text)',
            '\n\nOutput only the CREATE SEMANTIC VIEW statement:'
        )
    ) INTO ai_response;

    -- Debug: Verify AI response length
    IF (:ai_response IS NULL OR LENGTH(:ai_response) = 0) THEN
        RETURN 'Error: AI_COMPLETE returned an empty response. Column info: ' || SUBSTRING(:column_info, 1, 500);
    END IF;

    -- Remove unnecessary characters
    create_view_sql := :ai_response;
    create_view_sql := REPLACE(:create_view_sql, '

```sql', '');
    create_view_sql := REPLACE(:create_view_sql, '```

', '');
    create_view_sql := REPLACE(:create_view_sql, '"', '''');
    create_view_sql := REPLACE(:create_view_sql, 'Revised:', '');
    create_view_sql := REPLACE(:create_view_sql, 'Modifications:', '');
    create_view_sql := REPLACE(:create_view_sql, '**', '');
    create_view_sql := TRIM(:create_view_sql);

    -- Final verification: Check SQL statement is not empty
    IF (:create_view_sql IS NULL OR LENGTH(:create_view_sql) < 50) THEN
        RETURN 'Error: Generated SQL is empty or too short. AI response: ' || SUBSTRING(:ai_response, 1, 1000);
    END IF;

    -- ================================================================================
    -- STEP 4: Create semantic view
    -- ================================================================================

    BEGIN
        EXECUTE IMMEDIATE :create_view_sql;
        RETURN 'Semantic view ' || :TARGET_DATABASE || '.' || :TARGET_SCHEMA || '.' || :TARGET_VIEW_NAME || ' created successfully. SQL: ' || :create_view_sql;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'Error: ' || SQLERRM || ' -- Generated SQL: ' || SUBSTRING(:create_view_sql, 1, 2000);
    END;

END;
$$;

-- ================================================================================
-- Usage Example
-- ================================================================================


-- Basic usage
CALL GENERATE_SEMANTIC_VIEW(
    'DEMO',
    'DEMO',
    'AQ',
    'DEM',
    'DEMO',
    'SV_AQ',
    'claude-sonnet-4-5'
);

-- Verify created semantic view
DESCRIBE SEMANTIC VIEW DEMO.DEMO.SV_AQ;

SELECT TRY_TO_DATE(DATEOBSERVED, 'YYYY-MM-DD') as observationdate
FROM aq;
