CREATE OR REPLACE MASKING POLICY latitude_mask AS (val float) returns float ->
  CASE
    WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
    ELSE 0.0
  END; 

CREATE OR REPLACE MASKING POLICY longitude_mask AS (val float) returns float ->
  CASE
    WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
    ELSE 0.0
  END;

CREATE OR REPLACE MASKING POLICY internal_only_mask AS (val string) returns string ->
  CASE
    WHEN current_role() IN ('ACCOUNTADMIN') THEN VAL
    ELSE '****'
  END;
  

show masking policies;

  drop masking policy latitude_mask;
  
  drop masking policy longitude_mask;
  
  select current_role();

 ----- '**.****'
  ---- Contacts in Data https://docs.snowflake.com/user-guide/contacts-using


  CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SENSITIVE_DATA_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE CONCAT(
        'masked-',
        SUBSTRING(val, 1, POSITION('@' IN val) -1),
        SUBSTRING(val, POSITION('@' IN val))
     )
  END;

  CREATE OR REPLACE MASKING POLICY phone_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SENSITIVE_DATA_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '***-***-' || RIGHT(val, 4)
  END;

  CREATE OR REPLACE MASKING POLICY ssn_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SENSITIVE_DATA_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '***-**-' || RIGHT(val, 4)
  END;

  
CREATE OR REPLACE MASKING POLICY date_mask AS (val DATE) RETURNS DATE ->
  CASE
    WHEN CURRENT_ROLE() IN ('SENSITIVE_DATA_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE DATE_TRUNC('YEAR', val)
  END;

  
CREATE OR REPLACE MASKING POLICY full_redact_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('SENSITIVE_DATA_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '**********'
  END;

  CREATE OR REPLACE MASKING POLICY number_mask_zero AS (val NUMBER) RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('SENSITIVE_DATA_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE 0
  END;

  CREATE OR REPLACE MASKING POLICY fuzz_number AS (val NUMBER) RETURNS NUMBER ->
CASE
    WHEN CURRENT_ROLE() IN ('SENSITIVE_DATA_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE val + (UNIFORM(-1, 1, RANDOM()) * 0.1 * val) -- Fuzz by up to 10%
END;


CREATE ROLE IF NOT EXISTS FINANCE_SENSITIVE_VIEWER;

GRANT ROLE FINANCE_SENSITIVE_VIEWER TO ROLE SYSADMIN;

CREATE OR REPLACE MASKING POLICY name_mask_initials AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE REGEXP_REPLACE(val, '(\\w)\\w*\\s*(\\w)?\\w*', '\\1. \\2.')
  END;

-- 2. SSN/TIN Masking (Last 4 digits visible)
CREATE OR REPLACE MASKING POLICY ssn_tin_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '***-**-' || RIGHT(val, 4)
  END;

-- 3. Date of Birth Masking (Show Year Only)
CREATE OR REPLACE MASKING POLICY dob_mask_year AS (val DATE) RETURNS DATE ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE DATE_TRUNC('YEAR', val)
  END;

-- 4. Full Address Redaction
CREATE OR REPLACE MASKING POLICY full_address_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '[REDACTED ADDRESS]'
  END;

-- 5. ZIP Code Masking (Show first 3 digits for US)
CREATE OR REPLACE MASKING POLICY zip_code_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE LEFT(val, 3) || '**'
  END;

-- 6. Email Masking (Hash user, keep domain)
CREATE OR REPLACE MASKING POLICY email_mask_hash AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE SHA2(SUBSTRING(val, 1, POSITION('@' IN val) - 1)) || SUBSTRING(val, POSITION('@' IN val))
  END;

-- 7. Phone Number Masking (Last 4 digits visible)
CREATE OR REPLACE MASKING POLICY phone_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '***-***-' || RIGHT(val, 4)
  END;

-- 8. IP Address Masking (Anonymize last octet)
CREATE OR REPLACE MASKING POLICY ip_address_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE REGEXP_REPLACE(val, '\\.\\d+$', '.***')
  END;


-- ----------------------------------------------------------------------------
-- Section 2: Financial Account & Instrument Data
-- ----------------------------------------------------------------------------

-- 9. Account Number Masking (Last 4 digits visible)
CREATE OR REPLACE MASKING POLICY account_number_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '****-****-' || RIGHT(val, 4)
  END;

-- 10. Credit Card Number Masking (PCI DSS compliant)
CREATE OR REPLACE MASKING POLICY credit_card_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '************' || RIGHT(val, 4)
  END;

-- 11. Bank Routing Number Redaction
CREATE OR REPLACE MASKING POLICY routing_number_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '*********'
  END;
  
-- 12. CUSIP/ISIN/Ticker Symbol Redaction
CREATE OR REPLACE MASKING POLICY security_symbol_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '[REDACTED SYMBOL]'
  END;

-- 13. Client ID / Trader ID Hashing
CREATE OR REPLACE MASKING POLICY id_hash_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE SHA2(val)
  END;


-- ----------------------------------------------------------------------------
-- Section 3: Transactional & Analytical Data
-- ----------------------------------------------------------------------------

-- 15. Transaction Amount Fuzzing (Randomize +/- 10%)
CREATE OR REPLACE MASKING POLICY amount_fuzzing_mask AS (val FLOAT) RETURNS FLOAT ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE val * (1 + UNIFORM(-0.1::FLOAT, 0.1::FLOAT, RANDOM()))
  END;

-- 16. Balance / Portfolio Value Redaction (Zeroing out)
CREATE OR REPLACE MASKING POLICY balance_zero_mask AS (val NUMBER(38,2)) RETURNS NUMBER(38,2) ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE 0.00
  END;


  
-- 18. Geolocation Masking (Generalize coordinates)
CREATE OR REPLACE MASKING POLICY geo_mask AS (val GEOGRAPHY) RETURNS GEOGRAPHY ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE ST_MAKEPOINT(ROUND(ST_X(val), 2), ROUND(ST_Y(val), 2))
  END;
  
-- 19. Timestamp Masking (Show Date Only)
CREATE OR REPLACE MASKING POLICY timestamp_date_only_mask AS (val TIMESTAMP) RETURNS TIMESTAMP ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE DATE_TRUNC('DAY', val)::TIMESTAMP
  END;



-- ----------------------------------------------------------------------------
-- Section 4: Advanced & Unstructured Data
-- ----------------------------------------------------------------------------

-- 21. Full String Redaction
CREATE OR REPLACE MASKING POLICY full_redact_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE '**********'
  END;
  


-- 23. Hashing Policy (Generic for any string)
CREATE OR REPLACE MASKING POLICY hash_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE SHA2(val)
  END;

-- 24. Nullifying Mask (Replace value with NULL)
CREATE OR REPLACE MASKING POLICY nullify_string AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE NULL
  END;
  
-- Note: 'T' is a placeholder. You need to create this for each data type:
-- CREATE OR REPLACE MASKING POLICY nullify_string AS (val STRING) ...
-- CREATE OR REPLACE MASKING POLICY nullify_number AS (val NUMBER) ...

-- 25. Fixed Value Mask (Replace with a constant)
CREATE OR REPLACE MASKING POLICY fixed_value_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('FINANCE_SENSITIVE_VIEWER', 'ACCOUNTADMIN') THEN val
    ELSE 'Not Applicable'
  END;

