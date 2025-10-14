-- https://docs.snowflake.com/en/user-guide/object-tagging/introduction 

CREATE TAG BRX_TAG COMMENT = 'BRX standard field';

-- Snowflake SQL Script to Create Semantic Tags
-- Generated based on the provided CSV file.
-- These tags can be used for data classification, governance, and security policies.

use database DEMO;
use schema DEMO;

-- ==================================================
-- Category: PII (Personally Identifiable Information)
-- ==================================================
CREATE OR REPLACE TAG PII_FULL_NAME COMMENT = 'Full name of an individual.';
CREATE OR REPLACE TAG PII_FIRST_NAME COMMENT = 'First name of an individual.';
CREATE OR REPLACE TAG PII_LAST_NAME COMMENT = 'Last name of an individual.';
CREATE OR REPLACE TAG PII_EMAIL COMMENT = 'Email address of an individual.';
CREATE OR REPLACE TAG PII_PHONE_MOBILE COMMENT = 'Mobile phone number.';
CREATE OR REPLACE TAG PII_PHONE_HOME COMMENT = 'Home phone number.';
CREATE OR REPLACE TAG PII_SSN COMMENT = 'Social Security Number.';
CREATE OR REPLACE TAG PII_DRIVERS_LICENSE COMMENT = 'Driver''s license number.';
CREATE OR REPLACE TAG PII_PASSPORT_NUMBER COMMENT = 'Passport number.';
CREATE OR REPLACE TAG PII_NATIONAL_ID COMMENT = 'National identification number.';
CREATE OR REPLACE TAG PII_TAX_ID COMMENT = 'Taxpayer identification number.';
CREATE OR REPLACE TAG PII_DOB COMMENT = 'Date of Birth.';
CREATE OR REPLACE TAG PII_AGE COMMENT = 'Age of an individual.';
CREATE OR REPLACE TAG PII_GENDER COMMENT = 'Gender or gender identity.';
CREATE OR REPLACE TAG PII_RACE_ETHNICITY COMMENT = 'Race or ethnicity of an individual.';
CREATE OR REPLACE TAG PII_IP_ADDRESS COMMENT = 'Internet Protocol (IP) address.';
CREATE OR REPLACE TAG PII_MAC_ADDRESS COMMENT = 'Media Access Control (MAC) address.';
CREATE OR REPLACE TAG PII_USERNAME COMMENT = 'Username for a system or service.';
CREATE OR REPLACE TAG PII_USER_ID COMMENT = 'Unique identifier for a user.';
CREATE OR REPLACE TAG PII_BIOMETRIC COMMENT = 'Biometric data like fingerprints or retinal scans.';

-- =============================================
-- Category: PHI (Protected Health Information)
-- =============================================
CREATE OR REPLACE TAG PHI_MEDICAL_RECORD_NUMBER COMMENT = 'Medical Record Number (MRN).';
CREATE OR REPLACE TAG PHI_HEALTH_PLAN_ID COMMENT = 'Health insurance plan or policy number.';
CREATE OR REPLACE TAG PHI_PATIENT_ID COMMENT = 'Unique identifier for a patient.';
CREATE OR REPLACE TAG PHI_DIAGNOSIS_CODE COMMENT = 'Medical diagnosis code (e.g., ICD-10).';
CREATE OR REPLACE TAG PHI_TREATMENT_INFO COMMENT = 'Information about medical treatments received.';
CREATE OR REPLACE TAG PHI_PRESCRIPTION COMMENT = 'Prescription medication details.';

-- ==========================
-- Category: Financial
-- ==========================
CREATE OR REPLACE TAG FIN_CREDIT_CARD_NUMBER COMMENT = 'Primary Account Number (PAN) of a credit or debit card.';
CREATE OR REPLACE TAG FIN_BANK_ACCOUNT_NUMBER COMMENT = 'Bank account number (e.g., IBAN, Routing Number).';
CREATE OR REPLACE TAG FIN_CREDIT_SCORE COMMENT = 'Credit score of an individual.';

-- ==========================
-- Category: Location
-- ==========================
CREATE OR REPLACE TAG LOC_STREET_ADDRESS COMMENT = 'Full street address line.';
CREATE OR REPLACE TAG LOC_CITY COMMENT = 'City name.';
CREATE OR REPLACE TAG LOC_STATE_PROVINCE COMMENT = 'State or province name/code.';
CREATE OR REPLACE TAG LOC_ZIP_POSTAL_CODE COMMENT = 'ZIP or postal code.';
CREATE OR REPLACE TAG LOC_COUNTY COMMENT = 'County name.';
CREATE OR REPLACE TAG LOC_COUNTRY COMMENT = 'Country name or code (e.g., ISO 3166-1).';
CREATE OR REPLACE TAG LOC_LATITUDE COMMENT = 'Latitude coordinate.';
CREATE OR REPLACE TAG LOC_LONGITUDE COMMENT = 'Longitude coordinate.';
CREATE OR REPLACE TAG LOC_TIMEZONE COMMENT = 'Timezone identifier (e.g., ''America/New_York'').';

-- ==========================
-- Category: Currency
-- ==========================
CREATE OR REPLACE TAG CUR_AMOUNT COMMENT = 'A monetary amount.';
CREATE OR REPLACE TAG CUR_PRICE COMMENT = 'The price of an item or service.';
CREATE OR REPLACE TAG CUR_COST COMMENT = 'The cost of an item or service.';
CREATE OR REPLACE TAG CUR_REVENUE COMMENT = 'Revenue amount.';
CREATE OR REPLACE TAG CUR_BALANCE COMMENT = 'Account or financial balance.';
CREATE OR REPLACE TAG CUR_TAX COMMENT = 'Tax amount.';
CREATE OR REPLACE TAG CUR_DISCOUNT COMMENT = 'Discount amount.';
CREATE OR REPLACE TAG CUR_CURRENCY_CODE COMMENT = 'ISO 4217 currency code (e.g., USD, EUR).';

-- ==========================
-- Category: Date/Time
-- ==========================
CREATE OR REPLACE TAG TIME_DATE COMMENT = 'A calendar date (YYYY-MM-DD).';
CREATE OR REPLACE TAG TIME_DATETIME_UTC COMMENT = 'A specific date and time in Coordinated Universal Time (UTC).';
CREATE OR REPLACE TAG TIME_DATETIME_LOCAL COMMENT = 'A specific date and time with timezone information.';
CREATE OR REPLACE TAG TIME_TIMESTAMP COMMENT = 'A timestamp representing a point in time.';
CREATE OR REPLACE TAG TIME_YEAR COMMENT = 'The year component of a date.';
CREATE OR REPLACE TAG TIME_QUARTER COMMENT = 'The quarter component of a date (1-4).';
CREATE OR REPLACE TAG TIME_MONTH COMMENT = 'The month component of a date.';
CREATE OR REPLACE TAG TIME_DAY COMMENT = 'The day component of a date.';
CREATE OR REPLACE TAG TIME_HOUR COMMENT = 'The hour component of a time.';
CREATE OR REPLACE TAG TIME_FISCAL_YEAR COMMENT = 'Fiscal year designation.';
CREATE OR REPLACE TAG TIME_FISCAL_QUARTER COMMENT = 'Fiscal quarter designation.';

-- ==========================
-- Category: Transaction
-- ==========================
CREATE OR REPLACE TAG TXN_TRANSACTION_ID COMMENT = 'Unique identifier for a transaction.';
CREATE OR REPLACE TAG TXN_ORDER_ID COMMENT = 'Unique identifier for a customer order.';
CREATE OR REPLACE TAG TXN_INVOICE_ID COMMENT = 'Unique identifier for an invoice.';
CREATE OR REPLACE TAG TXN_SHIPMENT_ID COMMENT = 'Unique identifier for a shipment.';
CREATE OR REPLACE TAG TXN_CUSTOMER_ID COMMENT = 'Identifier for the customer involved in the transaction.';
CREATE OR REPLACE TAG TXN_PRODUCT_ID COMMENT = 'Identifier for the product involved in the transaction.';
CREATE OR REPLACE TAG TXN_SKU COMMENT = 'Stock Keeping Unit for a product.';
CREATE OR REPLACE TAG TXN_PAYMENT_METHOD COMMENT = 'Method of payment used (e.g., ''Credit Card'', ''PayPal'').';
CREATE OR REPLACE TAG TXN_ORDER_STATUS COMMENT = 'Current status of an order (e.g., ''Shipped'', ''Pending'').';
CREATE OR REPLACE TAG TXN_TRANSACTION_TYPE COMMENT = 'Type of transaction (e.g., ''Sale'', ''Refund'').';

-- ==========================
-- Category: Business
-- ==========================
CREATE OR REPLACE TAG BIZ_COMPANY_NAME COMMENT = 'The legal name of a company.';
CREATE OR REPLACE TAG BIZ_DEPARTMENT COMMENT = 'A department within an organization.';
CREATE OR REPLACE TAG BIZ_EMPLOYEE_ID COMMENT = 'Unique identifier for an employee.';
CREATE OR REPLACE TAG BIZ_JOB_TITLE COMMENT = 'Job title or position of an employee.';
CREATE OR REPLACE TAG BIZ_STORE_ID COMMENT = 'Unique identifier for a retail store or location.';
CREATE OR REPLACE TAG BIZ_VENDOR_ID COMMENT = 'Unique identifier for a vendor or supplier.';
CREATE OR REPLACE TAG BIZ_ACCOUNT_ID COMMENT = 'Unique identifier for a business account.';
CREATE OR REPLACE TAG BIZ_CONTRACT_NUMBER COMMENT = 'Identifier for a legal contract.';
CREATE OR REPLACE TAG BIZ_STOCK_TICKER COMMENT = 'Stock market ticker symbol.';

-- ==========================
-- Category: Technical
-- ==========================
CREATE OR REPLACE TAG TECH_URL COMMENT = 'Uniform Resource Locator (URL).';
CREATE OR REPLACE TAG TECH_USER_AGENT COMMENT = 'User-Agent string from a web browser.';
CREATE OR REPLACE TAG TECH_COOKIE_ID COMMENT = 'Identifier stored in a browser cookie.';
CREATE OR REPLACE TAG TECH_SESSION_ID COMMENT = 'Identifier for a user''s session.';
CREATE OR REPLACE TAG TECH_HOSTNAME COMMENT = 'Hostname of a server or device.';
CREATE OR REPLACE TAG TECH_API_KEY COMMENT = 'API key for authentication.';
CREATE OR REPLACE TAG TECH_AUTH_TOKEN COMMENT = 'Authentication token.';
CREATE OR REPLACE TAG TECH_LOG_MESSAGE COMMENT = 'A message from a log file.';
CREATE OR REPLACE TAG TECH_ERROR_CODE COMMENT = 'An error code from a system or application.';
CREATE OR REPLACE TAG TECH_FILE_PATH COMMENT = 'A path to a file in a filesystem.';

-- ==========================
-- Category: General
-- ==========================
CREATE OR REPLACE TAG GEN_IDENTIFIER COMMENT = 'A generic unique identifier.';
CREATE OR REPLACE TAG GEN_CODE COMMENT = 'A generic code or abbreviation.';
CREATE OR REPLACE TAG GEN_NAME COMMENT = 'A generic name for an entity.';
CREATE OR REPLACE TAG GEN_DESCRIPTION COMMENT = 'A textual description of an entity.';
CREATE OR REPLACE TAG GEN_TYPE COMMENT = 'A type classification for an entity.';
CREATE OR REPLACE TAG GEN_CATEGORY COMMENT = 'A category classification for an entity.';
CREATE OR REPLACE TAG GEN_STATUS COMMENT = 'A status indicator.';
CREATE OR REPLACE TAG GEN_FLAG COMMENT = 'A boolean flag (true/false).';
CREATE OR REPLACE TAG GEN_QUANTITY COMMENT = 'A measure of quantity or count.';
CREATE OR REPLACE TAG GEN_VERSION COMMENT = 'A version number or string.';
CREATE OR REPLACE TAG GEN_COMMENT COMMENT = 'A free-text comment or note.';

show tags;
