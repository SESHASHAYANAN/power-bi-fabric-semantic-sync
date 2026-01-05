-- ============================================================
-- SNOWFLAKE SCHEDULED TASKS
-- Fabric-Snowflake Semantic Model Synchronization
-- Created: 2026-01-03
-- ============================================================

USE SCHEMA SYNC_OPERATIONS;

-- ============================================================
-- 1. CREATE DEDICATED WAREHOUSE FOR SYNC OPERATIONS
-- ============================================================
CREATE WAREHOUSE IF NOT EXISTS SEMANTIC_SYNC_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for Fabric-Snowflake semantic model synchronization';

-- ============================================================
-- 2. MAIN BIDIRECTIONAL SYNC TASK - Every 1 Hour
-- ============================================================
CREATE OR REPLACE TASK TASK_BIDIRECTIONAL_SYNC
    WAREHOUSE = SEMANTIC_SYNC_WH
    SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour at minute 0
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 1800000  -- 30 minute timeout
    ERROR_INTEGRATION = SYNC_ERROR_NOTIFICATION
    COMMENT = 'Bidirectional semantic model sync between Fabric and Snowflake - runs hourly'
AS
DECLARE
    sync_result VARIANT;
    webhook_url VARCHAR := 'https://your-azure-function-app.azurewebsites.net/api/fabric-snowflake-sync';
BEGIN
    -- Execute the main sync procedure
    CALL SP_EXECUTE_SEMANTIC_SYNC('BIDIRECTIONAL', :webhook_url, FALSE, FALSE) INTO :sync_result;
    
    -- Log the result
    INSERT INTO SYNC_METRICS (
        metric_name, 
        metric_value, 
        metric_unit, 
        sync_id, 
        sync_direction
    )
    SELECT 
        'task_execution_status',
        CASE WHEN sync_result:status = 'COMPLETED' THEN 1 ELSE 0 END,
        'boolean',
        sync_result:sync_id::VARCHAR,
        'BIDIRECTIONAL';
    
    -- If failed, attempt retry
    IF (sync_result:status = 'FAILED' AND sync_result:retry_count < 3) THEN
        CALL SP_RETRY_FAILED_SYNC(sync_result:sync_id::VARCHAR, :webhook_url);
    END IF;
    
    RETURN sync_result;
END;

-- ============================================================
-- 3. FABRIC TO SNOWFLAKE SYNC TASK - Off-Peak Hours (2 AM UTC)
-- ============================================================
CREATE OR REPLACE TASK TASK_FABRIC_TO_SNOWFLAKE_FULL_SYNC
    WAREHOUSE = SEMANTIC_SYNC_WH
    SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- 2 AM UTC daily
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 3600000  -- 1 hour timeout for full sync
    ERROR_INTEGRATION = SYNC_ERROR_NOTIFICATION
    COMMENT = 'Full sync from Fabric to Snowflake - runs daily at 2 AM UTC (off-peak)'
AS
DECLARE
    sync_result VARIANT;
    webhook_url VARCHAR := 'https://your-azure-function-app.azurewebsites.net/api/fabric-snowflake-sync';
BEGIN
    -- Force full sync during off-peak hours
    CALL SP_EXECUTE_SEMANTIC_SYNC('FABRIC_TO_SNOWFLAKE', :webhook_url, FALSE, TRUE) INTO :sync_result;
    
    -- Check and send notifications
    CALL SP_CHECK_AND_NOTIFY_FAILURES();
    
    RETURN sync_result;
END;

-- ============================================================
-- 4. SNOWFLAKE TO FABRIC SYNC TASK - Off-Peak Hours (3 AM UTC)
-- ============================================================
CREATE OR REPLACE TASK TASK_SNOWFLAKE_TO_FABRIC_FULL_SYNC
    WAREHOUSE = SEMANTIC_SYNC_WH
    SCHEDULE = 'USING CRON 0 3 * * * UTC'  -- 3 AM UTC daily
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 3600000  -- 1 hour timeout
    ERROR_INTEGRATION = SYNC_ERROR_NOTIFICATION
    COMMENT = 'Full sync from Snowflake to Fabric - runs daily at 3 AM UTC (off-peak)'
AS
DECLARE
    sync_result VARIANT;
    webhook_url VARCHAR := 'https://your-azure-function-app.azurewebsites.net/api/fabric-snowflake-sync';
BEGIN
    CALL SP_EXECUTE_SEMANTIC_SYNC('SNOWFLAKE_TO_FABRIC', :webhook_url, FALSE, TRUE) INTO :sync_result;
    
    CALL SP_CHECK_AND_NOTIFY_FAILURES();
    
    RETURN sync_result;
END;

-- ============================================================
-- 5. HEALTH CHECK TASK - Every 15 Minutes
-- ============================================================
CREATE OR REPLACE TASK TASK_SYNC_HEALTH_CHECK
    WAREHOUSE = SEMANTIC_SYNC_WH
    SCHEDULE = 'USING CRON */15 * * * * UTC'  -- Every 15 minutes
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 120000  -- 2 minute timeout
    COMMENT = 'Health check for sync operations - runs every 15 minutes'
AS
DECLARE
    health_result VARIANT;
BEGIN
    CALL SP_SYNC_HEALTH_CHECK() INTO :health_result;
    
    -- If health check detected critical issues, send notifications
    IF (health_result:overall_status = 'CRITICAL') THEN
        CALL SP_CHECK_AND_NOTIFY_FAILURES();
    END IF;
    
    -- Log health status as metric
    INSERT INTO SYNC_METRICS (
        metric_name,
        metric_value,
        metric_unit,
        sync_direction,
        dimension_1_name,
        dimension_1_value
    )
    SELECT 
        'health_check_overall',
        CASE 
            WHEN health_result:overall_status = 'HEALTHY' THEN 1
            WHEN health_result:overall_status = 'WARNING' THEN 0.5
            ELSE 0
        END,
        'status_score',
        'HEALTH_CHECK',
        'status',
        health_result:overall_status::VARCHAR;
    
    RETURN health_result;
END;

-- ============================================================
-- 6. INCREMENTAL CHANGE DETECTION TASK - Every 1 Hour
-- ============================================================
CREATE OR REPLACE TASK TASK_INCREMENTAL_CHANGE_DETECTION
    WAREHOUSE = SEMANTIC_SYNC_WH
    SCHEDULE = 'USING CRON 30 * * * * UTC'  -- Every hour at minute 30
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 600000  -- 10 minute timeout
    COMMENT = 'Incremental change detection - runs every hour at :30'
AS
DECLARE
    detection_result VARIANT;
    webhook_url VARCHAR := 'https://your-azure-function-app.azurewebsites.net/api/detect-changes';
BEGIN
    -- Call webhook for change detection
    -- This triggers the Python change_detector.py script
    
    -- Log the detection attempt
    INSERT INTO SYNC_METRICS (
        metric_name,
        metric_value,
        metric_unit,
        sync_direction
    )
    VALUES (
        'change_detection_triggered',
        1,
        'count',
        'BIDIRECTIONAL'
    );
    
    RETURN OBJECT_CONSTRUCT(
        'status', 'TRIGGERED',
        'timestamp', CURRENT_TIMESTAMP()::VARCHAR,
        'webhook_url', webhook_url
    );
END;

-- ============================================================
-- 7. CLEANUP TASK - Daily at 4 AM UTC
-- ============================================================
CREATE OR REPLACE TASK TASK_CLEANUP_OLD_RECORDS
    WAREHOUSE = SEMANTIC_SYNC_WH
    SCHEDULE = 'USING CRON 0 4 * * * UTC'  -- 4 AM UTC daily
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 600000  -- 10 minute timeout
    COMMENT = 'Cleanup old audit records - runs daily at 4 AM UTC'
AS
DECLARE
    cleanup_result VARIANT;
BEGIN
    CALL SP_CLEANUP_OLD_RECORDS(90) INTO :cleanup_result;
    
    -- Log cleanup metrics
    INSERT INTO SYNC_METRICS (
        metric_name,
        metric_value,
        metric_unit,
        sync_direction
    )
    SELECT 
        'cleanup_records_deleted',
        NVL(cleanup_result:records_deleted:audit_logs::NUMBER, 0) + 
        NVL(cleanup_result:records_deleted:metrics::NUMBER, 0) +
        NVL(cleanup_result:records_deleted:resolved_errors::NUMBER, 0),
        'count',
        'MAINTENANCE';
    
    RETURN cleanup_result;
END;

-- ============================================================
-- 8. AUTO-RETRY FAILED SYNCS TASK - Every 15 Minutes
-- ============================================================
CREATE OR REPLACE TASK TASK_AUTO_RETRY_FAILED
    WAREHOUSE = SEMANTIC_SYNC_WH
    SCHEDULE = 'USING CRON */15 * * * * UTC'  -- Every 15 minutes
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    USER_TASK_TIMEOUT_MS = 300000  -- 5 minute timeout
    COMMENT = 'Automatically retry failed sync operations - runs every 15 minutes'
AS
DECLARE
    failed_syncs ARRAY;
    sync_id VARCHAR;
    retry_result VARIANT;
    webhook_url VARCHAR := 'https://your-azure-function-app.azurewebsites.net/api/fabric-snowflake-sync';
    retried_count NUMBER := 0;
BEGIN
    -- Find failed syncs eligible for retry
    LET cur CURSOR FOR
        SELECT sync_id, sync_direction
        FROM SYNC_AUDIT_LOG
        WHERE sync_status = 'FAILED'
        AND retry_count < max_retries
        AND sync_timestamp >= DATEADD('HOUR', -2, CURRENT_TIMESTAMP())
        ORDER BY sync_timestamp DESC
        LIMIT 5;  -- Limit to 5 retries per run
    
    FOR record IN cur DO
        CALL SP_RETRY_FAILED_SYNC(record.sync_id, :webhook_url) INTO :retry_result;
        retried_count := retried_count + 1;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'retried_count', retried_count,
        'timestamp', CURRENT_TIMESTAMP()::VARCHAR
    );
END;

-- ============================================================
-- 9. CREATE ERROR NOTIFICATION INTEGRATION
-- ============================================================
-- Note: This requires appropriate cloud provider setup

-- For AWS SNS:
-- CREATE OR REPLACE NOTIFICATION INTEGRATION SYNC_ERROR_NOTIFICATION
--     ENABLED = TRUE
--     TYPE = QUEUE
--     NOTIFICATION_PROVIDER = AWS_SNS
--     DIRECTION = OUTBOUND
--     AWS_SNS_TOPIC_ARN = 'arn:aws:sns:region:account:sync-errors-topic'
--     AWS_SNS_ROLE_ARN = 'arn:aws:iam::account:role/snowflake-sns-role';

-- For Azure Event Grid:
-- CREATE OR REPLACE NOTIFICATION INTEGRATION SYNC_ERROR_NOTIFICATION
--     ENABLED = TRUE
--     TYPE = QUEUE
--     NOTIFICATION_PROVIDER = AZURE_EVENT_GRID
--     DIRECTION = OUTBOUND
--     AZURE_EVENT_GRID_TOPIC_ENDPOINT = 'https://your-event-grid.region.eventgrid.azure.net/api/events'
--     AZURE_TENANT_ID = 'your-tenant-id';

-- Placeholder integration (replace with actual setup)
CREATE OR REPLACE NOTIFICATION INTEGRATION SYNC_ERROR_NOTIFICATION
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('data-engineering-team@company.com', 'devops@company.com')
    COMMENT = 'Email notifications for sync task errors';

-- ============================================================
-- 10. ENABLE ALL TASKS
-- ============================================================

-- Resume tasks (they are created in suspended state by default)
ALTER TASK TASK_BIDIRECTIONAL_SYNC RESUME;
ALTER TASK TASK_FABRIC_TO_SNOWFLAKE_FULL_SYNC RESUME;
ALTER TASK TASK_SNOWFLAKE_TO_FABRIC_FULL_SYNC RESUME;
ALTER TASK TASK_SYNC_HEALTH_CHECK RESUME;
ALTER TASK TASK_INCREMENTAL_CHANGE_DETECTION RESUME;
ALTER TASK TASK_CLEANUP_OLD_RECORDS RESUME;
ALTER TASK TASK_AUTO_RETRY_FAILED RESUME;

-- ============================================================
-- 11. CREATE TASK DEPENDENCY GRAPH (Optional)
-- ============================================================

-- Create a stream to track changes for event-driven sync
CREATE OR REPLACE STREAM STREAM_SEMANTIC_VIEW_CHANGES
    ON TABLE SEMANTIC_LAYER.MODEL_METADATA
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE;

-- Task that triggers on view changes
CREATE OR REPLACE TASK TASK_ON_VIEW_CHANGE
    WAREHOUSE = SEMANTIC_SYNC_WH
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_SEMANTIC_VIEW_CHANGES')
    COMMENT = 'Event-driven sync triggered by view changes'
AS
DECLARE
    change_count NUMBER;
BEGIN
    -- Count changes
    SELECT COUNT(*) INTO :change_count FROM STREAM_SEMANTIC_VIEW_CHANGES;
    
    IF (change_count > 0) THEN
        -- Log the changes
        INSERT INTO MODEL_CHANGE_HISTORY (
            model_name, source_system, change_type, item_type,
            detected_by
        )
        SELECT 
            $1, 'SNOWFLAKE', 'MODIFIED', 'VIEW',
            'STREAM_DETECTION'
        FROM STREAM_SEMANTIC_VIEW_CHANGES;
        
        -- Trigger incremental sync
        -- CALL SP_EXECUTE_SEMANTIC_SYNC('SNOWFLAKE_TO_FABRIC', NULL, FALSE, FALSE);
    END IF;
    
    RETURN OBJECT_CONSTRUCT('changes_detected', change_count);
END;

-- ============================================================
-- 12. MONITORING QUERIES
-- ============================================================

-- View current task status
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 50;

-- View task schedules
-- SHOW TASKS IN SCHEMA SYNC_OPERATIONS;

-- View task run history
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('DAY', -1, CURRENT_TIMESTAMP()),
--     RESULT_LIMIT => 100
-- ))
-- ORDER BY SCHEDULED_TIME DESC;

-- Check next scheduled runs
-- SELECT 
--     NAME,
--     STATE,
--     SCHEDULE,
--     LAST_COMMITTED_ON,
--     NEXT_SCHEDULED_TIME
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
-- WHERE STATE = 'started' OR STATE = 'scheduled'
-- ORDER BY NEXT_SCHEDULED_TIME;

COMMENT ON SCHEMA SYNC_OPERATIONS IS 'Schema containing all Fabric-Snowflake synchronization infrastructure including tasks, procedures, and audit tables';
