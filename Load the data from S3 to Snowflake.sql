-- ====================================================================================================
-- SNOWFLAKE E-COMMERCE SALES DATA PIPELINE
-- ====================================================================================================
-- Author: Milan Pansuriya
-- Repository: https://github.com/MilanPansuriya16/Snowflake.git
-- Description: Complete data pipeline from AWS S3 to Snowflake with automated CDC using Streams & Tasks
-- Use Case: Load sales data from S3, transform it, and maintain a clean analytics table
-- ====================================================================================================

-- ====================================================================================================
-- SECTION 1: DATABASE AND SCHEMA SETUP
-- ====================================================================================================
-- Purpose: Create isolated environment for e-commerce data
-- Best Practice: Separate databases for different business domains

-- Create the main database for e-commerce data
CREATE DATABASE ECOMMERCE_DEMO;

-- Create RAW schema to store unprocessed data from external sources
-- Naming Convention: RAW = untransformed data, STAGING = intermediate, PROD = production-ready
CREATE SCHEMA RAW;

-- Switch context to our newly created database and schema
-- This sets the working environment for all subsequent commands
USE DATABASE ECOMMERCE_DEMO;
USE SCHEMA RAW;

-- Verification: Check current context
SELECT 
    CURRENT_DATABASE() AS current_db,
    CURRENT_SCHEMA() AS current_schema,
    CURRENT_USER() AS user,
    CURRENT_ROLE() AS role;


-- ====================================================================================================
-- SECTION 2: TABLE CREATION
-- ====================================================================================================
-- Purpose: Create staging table to receive raw data from S3
-- Note: Using STRING type initially to handle any data quality issues during ingestion

-- Create raw sales table with flexible schema
-- Why STRING? Allows us to load data first, validate later (fail-safe approach)
CREATE TABLE SALES_RAW (
    DATE_TEXT STRING,      -- Raw date as text (will be converted to DATE later)
    SALES_TEXT STRING      -- Raw sales amount as text (will be converted to NUMBER later)
);

-- Expected Data Format:
-- DATE_TEXT: "2024-01-15" (YYYY-MM-DD format)
-- SALES_TEXT: "1500.50" (numeric values as strings)


-- ====================================================================================================
-- SECTION 3: FILE FORMAT DEFINITION
-- ====================================================================================================
-- Purpose: Define how Snowflake should parse CSV files from S3
-- This is reusable across multiple stages and tables

CREATE OR REPLACE FILE FORMAT SALE_CSV_FORMAT
    TYPE = 'CSV'                                    -- File type: CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'             -- Handle fields with quotes: "New York"
    FIELD_DELIMITER = ','                          -- Comma-separated values
    SKIP_HEADER = 1                                -- Skip first row (column names)
    NULL_IF = ('NULL', 'null', '');               -- Treat these values as NULL

-- What this handles:
-- ✅ Quoted fields: "John, Doe"
-- ✅ Empty values as NULL
-- ✅ Header row skip
-- ✅ Standard CSV format

-- Verify file format configuration
DESC FILE FORMAT SALE_CSV_FORMAT;


-- ====================================================================================================
-- SECTION 4: AWS S3 STORAGE INTEGRATION
-- ====================================================================================================
-- Purpose: Secure connection between Snowflake and AWS S3 using IAM roles (no hardcoded credentials)
-- Security: Uses temporary credentials via AWS STS AssumeRole

-- Create storage integration for AWS S3 access
-- This establishes trust between Snowflake and your AWS account
CREATE OR REPLACE STORAGE INTEGRATION AWS_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE                          -- External stage type (not internal Snowflake storage)
    STORAGE_PROVIDER = 'S3'                        -- Cloud provider: Amazon S3
    ENABLED = TRUE                                 -- Integration is active
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::651023585555:role/snowflake_s3_demo_role'  -- Your AWS IAM role
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-s3-demo-bucket/')  -- Allowed S3 bucket (trailing slash required!)
    COMMENT = 'Integration with AWS S3 for sales data';

-- IMPORTANT: After creating this integration, run DESC INTEGRATION to get credentials for AWS setup
-- You'll need these values to configure your IAM role trust policy in AWS:
-- 1. STORAGE_AWS_IAM_USER_ARN → Add to AWS IAM role trust policy Principal
-- 2. STORAGE_AWS_EXTERNAL_ID → Add to AWS IAM role trust policy Condition

-- Get integration details (copy these to AWS IAM role trust policy)
DESC INTEGRATION AWS_S3_INTEGRATION;

-- View all integrations in the account
SHOW INTEGRATIONS;

-- Expected Output:
-- STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::990176353941:user/qau82000-s (Snowflake's IAM user)
-- STORAGE_AWS_EXTERNAL_ID: ZU17907_SFCRole=4_xxxxx (Security token)
-- STORAGE_AWS_ROLE_ARN: Your role ARN


-- ====================================================================================================
-- SECTION 5: GRANT PERMISSIONS
-- ====================================================================================================
-- Purpose: Allow roles to use the integration and stage
-- Security Best Practice: Explicit permission grants following least-privilege principle

-- Grant integration usage to ACCOUNTADMIN role
-- This allows the role to create stages using this integration
GRANT USAGE ON INTEGRATION AWS_S3_INTEGRATION TO ROLE ACCOUNTADMIN;

-- Grant stage usage (must be done AFTER stage is created)
-- Note: This line will error if run before stage creation - that's normal!
GRANT USAGE ON STAGE AWS_STAGE TO ROLE ACCOUNTADMIN;

-- If working with multiple roles, grant to specific roles as needed:
-- GRANT USAGE ON INTEGRATION AWS_S3_INTEGRATION TO ROLE DATA_ENGINEER;
-- GRANT USAGE ON STAGE AWS_STAGE TO ROLE DATA_ENGINEER;


-- ====================================================================================================
-- SECTION 6: EXTERNAL STAGE CREATION
-- ====================================================================================================
-- Purpose: Create a named stage pointing to S3 bucket
-- Think of this as a "shortcut" or "pointer" to your S3 location

-- Create external stage linked to S3 bucket
CREATE OR REPLACE STAGE AWS_STAGE
    URL = 's3://snowflake-s3-demo-bucket/'        -- S3 bucket URL (trailing slash recommended)
    STORAGE_INTEGRATION = AWS_S3_INTEGRATION       -- Use the integration we created above
    FILE_FORMAT = SALE_CSV_FORMAT;                 -- Default file format for this stage

-- What is a Stage?
-- A stage is a Snowflake object that points to a file location (S3, Azure, GCS, or internal)
-- You can list, query, and load files from a stage

-- List all files in the S3 bucket through the stage
LIST @AWS_STAGE;

-- Expected Output: List of files with metadata (name, size, last modified date)
-- Example: s3://snowflake-s3-demo-bucket/daily_sales.csv | 1024 | 2024-01-15


-- ====================================================================================================
-- SECTION 7: DATA LOADING FROM S3 TO SNOWFLAKE
-- ====================================================================================================
-- Purpose: Copy data from S3 CSV files into Snowflake table
-- This is the ETL "Extract & Load" step

-- Load data from specific file in S3 stage into raw table
COPY INTO SALES_RAW
FROM @AWS_STAGE/daily_sales.csv                    -- Source: specific file in stage
FILE_FORMAT = SALE_CSV_FORMAT                      -- Use our CSV format definition
ON_ERROR = 'CONTINUE';                             -- Skip error rows and continue loading

-- ON_ERROR Options:
-- 'CONTINUE' → Skip bad rows, load good rows (use for partial loads)
-- 'SKIP_FILE' → Skip entire file if any error
-- 'ABORT_STATEMENT' → Stop and rollback on first error (default)

-- Alternative: Load ALL CSV files from stage
-- COPY INTO SALES_RAW FROM @AWS_STAGE FILE_FORMAT = SALE_CSV_FORMAT ON_ERROR = 'CONTINUE';

-- Verify data loaded successfully
SELECT * FROM SALES_RAW LIMIT 10;

-- Check load history and errors
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'SALES_RAW',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));


-- ====================================================================================================
-- SECTION 8: QUERY DATA WITH METADATA
-- ====================================================================================================
-- Purpose: Query files directly from stage with file metadata (useful for debugging and auditing)

-- Query stage files with metadata - useful for data lineage and troubleshooting
SELECT 
    METADATA$FILENAME AS file_name,                -- Which file this row came from
    METADATA$FILE_ROW_NUMBER AS row_number,        -- Row number within the file
    $1 AS date_column,                             -- First column (date)
    $2 AS sales_column                             -- Second column (sales amount)
FROM @AWS_STAGE
WHERE $1 <> 'date';                                -- Filter out header row if it exists

-- Metadata Functions:
-- METADATA$FILENAME → Source file name
-- METADATA$FILE_ROW_NUMBER → Row number in source file (useful for error tracking)
-- METADATA$FILE_LAST_MODIFIED → When file was last modified in S3

-- Use Case: If you find bad data in SALES_RAW, trace it back to source file and row


-- ====================================================================================================
-- SECTION 9: CHANGE DATA CAPTURE (CDC) WITH STREAMS
-- ====================================================================================================
-- Purpose: Track all changes (INSERT, UPDATE, DELETE) on SALES_RAW table
-- Streams enable real-time data pipelines and incremental processing

-- Create a stream on the raw sales table
-- Stream = Snowflake's CDC mechanism (Change Data Capture)
CREATE STREAM SALES_STREAM
    ON TABLE SALES_RAW;

-- How Streams Work:
-- 1. Stream captures all DML changes (INSERT, UPDATE, DELETE) after creation
-- 2. Changes are stored with METADATA$ACTION and METADATA$ISUPDATE columns
-- 3. When you query the stream, you see only NEW changes since last consumption
-- 4. After consuming (via INSERT/MERGE), the stream is "reset" for next batch

-- Stream Columns:
-- - All original table columns (DATE_TEXT, SALES_TEXT)
-- - METADATA$ACTION: INSERT, DELETE
-- - METADATA$ISUPDATE: TRUE if this is part of an UPDATE operation
-- - METADATA$ROW_ID: Unique identifier for the row

-- Test Stream (Uncomment to test):
/*
-- Insert test data
INSERT INTO SALES_RAW VALUES ('2024-12-06', '3200');

-- Query stream - should show the INSERT
SELECT * FROM SALES_STREAM;

-- Query original table
SELECT * FROM SALES_RAW;

-- Delete test data
DELETE FROM SALES_RAW WHERE DATE_TEXT = '2024-12-06';

-- Query stream again - should show the DELETE
SELECT * FROM SALES_STREAM;
*/


-- ====================================================================================================
-- SECTION 10: TARGET TABLE (CLEAN DATA)
-- ====================================================================================================
-- Purpose: Create production table with proper data types and transformations
-- This is the "analytics-ready" table that BI tools will query

-- Create clean sales table with proper data types
-- This table will have validated, transformed data
CREATE TABLE SALES_CLEAN (
    SALE_DATE DATE,                                -- Properly typed DATE column
    SALE_AMOUNT NUMBER(10, 2)                      -- Numeric with 2 decimal places (e.g., 1500.50)
);

-- Data Quality Improvements:
-- ✅ DATE type instead of STRING → Enables date functions and partitioning
-- ✅ NUMBER type instead of STRING → Enables aggregations (SUM, AVG)
-- ✅ Data validation during transformation (invalid dates/numbers will error)

-- Best Practice: Add constraints for data quality
-- ALTER TABLE SALES_CLEAN ADD CONSTRAINT chk_amount CHECK (SALE_AMOUNT >= 0);


-- ====================================================================================================
-- SECTION 11: AUTOMATED DATA PIPELINE WITH TASKS
-- ====================================================================================================
-- Purpose: Automate the process of merging changes from raw table to clean table
-- Task = Snowflake's job scheduler (like cron jobs)

-- Create automated task to merge stream data into clean table
-- This runs every 1 minute to process new changes
CREATE TASK SALES_MERGE_TASK
    WAREHOUSE = COMPUTE_WH                         -- Warehouse to execute the task (must be running)
    SCHEDULE = '1 minute'                          -- Run frequency (alternatives: '5 minute', 'USING CRON 0 9 * * * UTC')
AS 
    -- MERGE statement: Upsert logic (INSERT new, UPDATE existing, DELETE removed)
    MERGE INTO SALES_CLEAN TGT 
    USING (
        -- Transform raw data from stream
        SELECT 
            TRY_CAST(DATE_TEXT AS DATE) AS SALE_DATE,      -- Convert text to date (returns NULL if invalid)
            TRY_CAST(SALES_TEXT AS NUMBER) AS SALE_AMOUNT, -- Convert text to number (returns NULL if invalid)
            METADATA$ACTION AS ACTION,                      -- Type of change: INSERT, DELETE
            METADATA$ISUPDATE AS IS_UPDATE                  -- Is this part of an UPDATE?
        FROM SALES_STREAM
        WHERE SALE_DATE IS NOT NULL                         -- Data quality: skip invalid dates
          AND SALE_AMOUNT IS NOT NULL                       -- Data quality: skip invalid amounts
    ) SRC 
    ON TGT.SALE_DATE = SRC.SALE_DATE                       -- Match on business key (date)
    
    -- If row exists and was deleted in source → delete from target
    WHEN MATCHED AND SRC.ACTION = 'DELETE' THEN 
        DELETE
    
    -- If row exists and was updated in source → update in target
    WHEN MATCHED AND SRC.ACTION = 'INSERT' AND SRC.IS_UPDATE THEN 
        UPDATE SET TGT.SALE_AMOUNT = SRC.SALE_AMOUNT
    
    -- If row doesn't exist and was inserted in source → insert into target
    WHEN NOT MATCHED AND SRC.ACTION = 'INSERT' THEN
        INSERT (SALE_DATE, SALE_AMOUNT)
        VALUES (SRC.SALE_DATE, SRC.SALE_AMOUNT);

-- Task States:
-- SUSPENDED (default) → Task is created but not running
-- STARTED → Task is active and running on schedule

-- Task Scheduling Options:
-- '1 minute' → Every minute
-- '5 minute' → Every 5 minutes
-- 'USING CRON 0 9 * * * UTC' → Daily at 9:00 AM UTC
-- 'USING CRON 0 */4 * * * UTC' → Every 4 hours

-- IMPORTANT: Tasks are created in SUSPENDED state by default
-- You must RESUME them to start execution!


-- ====================================================================================================
-- SECTION 12: START THE TASK
-- ====================================================================================================
-- Purpose: Activate the task to start automated data processing

-- Resume (start) the task
-- After this command, the task will run every 1 minute automatically
ALTER TASK SALES_MERGE_TASK RESUME;

-- Task Management Commands:
-- ALTER TASK SALES_MERGE_TASK SUSPEND;  -- Stop the task
-- ALTER TASK SALES_MERGE_TASK RESUME;   -- Start the task
-- SHOW TASKS;                            -- List all tasks
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC; -- View execution history

-- Monitor Task Execution:
SELECT 
    NAME,
    STATE,
    SCHEDULE,
    WAREHOUSE,
    NEXT_SCHEDULED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'SALES_MERGE_TASK'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- Verify data in clean table
SELECT * FROM SALES_CLEAN ORDER BY SALE_DATE DESC;


-- ====================================================================================================
-- SECTION 13: TIME TRAVEL - DATA RECOVERY
-- ====================================================================================================
-- Purpose: Recover data from a previous point in time (Snowflake's time machine!)
-- Time Travel: Query/restore data as it existed at any point within retention period (default 1 day)

-- Scenario: You accidentally deleted rows or made bad updates
-- Solution: Clone the table from before the mistake happened

-- Method 1: Clone table from before a specific statement (query)
-- Step 1: Find the query ID that caused the problem
SELECT 
    QUERY_ID,
    QUERY_TEXT,
    START_TIME,
    ROWS_DELETED,
    ROWS_UPDATED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT ILIKE '%SALES_CLEAN%'
  AND (ROWS_DELETED > 0 OR ROWS_UPDATED > 0)
ORDER BY START_TIME DESC
LIMIT 10;

-- Step 2: Clone table to state BEFORE that query ran
CREATE TABLE SALES_CLEAN_RESTORE 
CLONE SALES_CLEAN
BEFORE (STATEMENT => '01a9f880-0000-1234-5678-abcdef123456');  -- Replace with actual QUERY_ID

-- Method 2: Clone table from specific timestamp
CREATE TABLE SALES_CLEAN_RESTORE 
CLONE SALES_CLEAN
AT (TIMESTAMP => '2024-01-15 14:30:00'::TIMESTAMP);

-- Method 3: Clone table from X minutes/hours/days ago
CREATE TABLE SALES_CLEAN_RESTORE 
CLONE SALES_CLEAN
AT (OFFSET => -60*5);  -- 5 minutes ago (offset in seconds)

-- Time Travel Limitations:
-- - Standard Edition: 1 day retention (configurable: 0-1 days)
-- - Enterprise Edition: 90 days retention (configurable: 0-90 days)
-- - Set retention: ALTER TABLE SALES_CLEAN SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Verify restored data
SELECT COUNT(*) FROM SALES_CLEAN_RESTORE;
SELECT * FROM SALES_CLEAN_RESTORE LIMIT 10;

-- If data looks good, replace the original table:
-- DROP TABLE SALES_CLEAN;
-- ALTER TABLE SALES_CLEAN_RESTORE RENAME TO SALES_CLEAN;


-- ====================================================================================================
-- SECTION 14: MONITORING AND MAINTENANCE
-- ====================================================================================================
-- Purpose: Queries to monitor your data pipeline health

-- 1. Check stream status and pending changes
SELECT SYSTEM$STREAM_HAS_DATA('SALES_STREAM') AS has_pending_changes;
SELECT COUNT(*) AS pending_changes FROM SALES_STREAM;

-- 2. View task execution history
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    RETURN_VALUE,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'SALES_MERGE_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD(hour, -24, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- 3. Check data freshness
SELECT 
    'SALES_RAW' AS table_name,
    COUNT(*) AS row_count,
    MAX(DATE_TEXT) AS latest_date
FROM SALES_RAW
UNION ALL
SELECT 
    'SALES_CLEAN' AS table_name,
    COUNT(*) AS row_count,
    MAX(SALE_DATE)::STRING AS latest_date
FROM SALES_CLEAN;

-- 4. View storage usage
SELECT 
    TABLE_NAME,
    TABLE_SCHEMA,
    ROW_COUNT,
    BYTES,
    ROUND(BYTES / 1024 / 1024, 2) AS MB
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'RAW'
ORDER BY BYTES DESC;

-- 5. Check warehouse credit usage (for cost monitoring)
SELECT 
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS total_credits,
    SUM(CREDITS_USED_COMPUTE) AS compute_credits,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_services_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME
ORDER BY total_credits DESC;


-- ====================================================================================================
-- SECTION 15: CLEANUP (Optional)
-- ====================================================================================================
-- Purpose: Clean up resources when done testing
-- ⚠️ WARNING: These commands will delete data! Use with caution!

-- Stop the task first (prevents it from running during cleanup)
-- ALTER TASK SALES_MERGE_TASK SUSPEND;

-- Drop objects in reverse order (due to dependencies)
-- DROP TASK IF EXISTS SALES_MERGE_TASK;
-- DROP STREAM IF EXISTS SALES_STREAM;
-- DROP TABLE IF EXISTS SALES_CLEAN;
-- DROP TABLE IF EXISTS SALES_CLEAN_RESTORE;
-- DROP TABLE IF EXISTS SALES_RAW;
-- DROP STAGE IF EXISTS AWS_STAGE;
-- DROP STORAGE INTEGRATION IF EXISTS AWS_S3_INTEGRATION;
-- DROP FILE FORMAT IF EXISTS SALE_CSV_FORMAT;
-- DROP SCHEMA IF EXISTS RAW;
-- DROP DATABASE IF EXISTS ECOMMERCE_DEMO;


-- ====================================================================================================
-- PIPELINE ARCHITECTURE SUMMARY
-- ====================================================================================================
/*
DATA FLOW:
1. AWS S3 (Source) → Contains CSV files with sales data
2. Storage Integration → Secure connection to S3 using IAM role
3. External Stage (AWS_STAGE) → Pointer to S3 bucket
4. SALES_RAW Table → Raw data staging (STRING types for flexibility)
5. SALES_STREAM → Captures all changes to SALES_RAW
6. SALES_MERGE_TASK (Automated) → Runs every 1 minute
7. SALES_CLEAN Table → Production-ready data (proper types, validated)

CHANGE DATA CAPTURE (CDC):
- Stream captures INSERT, UPDATE, DELETE operations
- Task processes stream data every minute
- MERGE handles upserts automatically
- Data transformation happens during MERGE (STRING → DATE/NUMBER)

BENEFITS:
✅ No credentials stored in Snowflake (uses IAM role)
✅ Automated data pipeline (hands-off operation)
✅ Real-time CDC (1-minute latency)
✅ Data validation during transformation
✅ Time Travel for recovery (up to 90 days)
✅ Scalable (handle millions of records)
✅ Cost-effective (pay-per-second compute)
*/

-- ====================================================================================================
-- END OF SCRIPT
-- ====================================================================================================
-- Questions or issues? Check troubleshooting guide:
-- Repository: https://github.com/MilanPansuriya16/Snowflake.git
-- Documentation: /docs/troubleshooting.md
-- ====================================================================================================
