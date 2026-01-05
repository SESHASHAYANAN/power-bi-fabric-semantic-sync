-- ============================================================
-- SNOWFLAKE AUDIT AND SYNC INFRASTRUCTURE
-- Fabric-Snowflake Semantic Model Synchronization
-- Created: 2026-01-03
-- ============================================================

-- ============================================================
-- 1. CREATE DEDICATED SCHEMA FOR SYNC OPERATIONS
-- ============================================================
CREATE SCHEMA IF NOT EXISTS SYNC_OPERATIONS;
USE SCHEMA SYNC_OPERATIONS;

-- ============================================================
-- 2. AUDIT TABLE - Logs all sync operations
-- ============================================================
CREATE OR REPLACE TABLE SYNC_AUDIT_LOG (
    log_id                  NUMBER AUTOINCREMENT PRIMARY KEY,
    sync_id                 VARCHAR(100) NOT NULL,
    sync_timestamp          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    sync_direction          VARCHAR(50) NOT NULL,  -- 'FABRIC_TO_SNOWFLAKE', 'SNOWFLAKE_TO_FABRIC', 'BIDIRECTIONAL'
    sync_status             VARCHAR(30) NOT NULL,  -- 'STARTED', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'PARTIAL'
    source_system           VARCHAR(50),
    target_system           VARCHAR(50),
    
    -- Operation details
    models_processed        NUMBER DEFAULT 0,
    views_created           NUMBER DEFAULT 0,
    views_updated           NUMBER DEFAULT 0,
    views_failed            NUMBER DEFAULT 0,
    measures_synced         NUMBER DEFAULT 0,
    
    -- Execution metrics
    execution_start_time    TIMESTAMP_NTZ,
    execution_end_time      TIMESTAMP_NTZ,
    execution_duration_ms   NUMBER,
    
    -- Error tracking
    error_code              VARCHAR(50),
    error_message           VARCHAR(4000),
    error_details           VARIANT,
    
    -- Retry information
    retry_count             NUMBER DEFAULT 0,
    max_retries             NUMBER DEFAULT 3,
    is_retry                BOOLEAN DEFAULT FALSE,
    parent_sync_id          VARCHAR(100),
    
    -- Metadata
    triggered_by            VARCHAR(100) DEFAULT 'SCHEDULED_TASK',
    environment             VARCHAR(50) DEFAULT 'PRODUCTION',
    task_name               VARCHAR(200),
    warehouse_used          VARCHAR(100),
    
    -- Audit fields
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 3. SYNC STATE TABLE - Tracks current sync state
-- ============================================================
CREATE OR REPLACE TABLE SYNC_STATE (
    state_id                NUMBER AUTOINCREMENT PRIMARY KEY,
    sync_type               VARCHAR(100) NOT NULL UNIQUE,
    last_successful_sync    TIMESTAMP_NTZ,
    last_attempted_sync     TIMESTAMP_NTZ,
    last_sync_status        VARCHAR(30),
    consecutive_failures    NUMBER DEFAULT 0,
    is_locked               BOOLEAN DEFAULT FALSE,
    lock_owner              VARCHAR(100),
    lock_acquired_at        TIMESTAMP_NTZ,
    lock_expires_at         TIMESTAMP_NTZ,
    
    -- State data
    current_state           VARIANT,
    checkpoint_data         VARIANT,
    
    -- Metadata
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert initial state records
INSERT INTO SYNC_STATE (sync_type, current_state) VALUES 
    ('FABRIC_TO_SNOWFLAKE', OBJECT_CONSTRUCT('initialized', TRUE)),
    ('SNOWFLAKE_TO_FABRIC', OBJECT_CONSTRUCT('initialized', TRUE)),
    ('BIDIRECTIONAL', OBJECT_CONSTRUCT('initialized', TRUE)),
    ('HEALTH_CHECK', OBJECT_CONSTRUCT('initialized', TRUE));

-- ============================================================
-- 4. MODEL CHANGE TRACKING TABLE
-- ============================================================
CREATE OR REPLACE TABLE MODEL_CHANGE_HISTORY (
    change_id               NUMBER AUTOINCREMENT PRIMARY KEY,
    model_name              VARCHAR(500) NOT NULL,
    model_id                VARCHAR(200),
    source_system           VARCHAR(50) NOT NULL,
    change_type             VARCHAR(30) NOT NULL,  -- 'ADDED', 'MODIFIED', 'DELETED'
    item_type               VARCHAR(50) NOT NULL,  -- 'TABLE', 'COLUMN', 'MEASURE', 'RELATIONSHIP'
    item_name               VARCHAR(500),
    table_name              VARCHAR(500),
    
    -- Change details
    old_value               VARIANT,
    new_value               VARIANT,
    change_diff             VARIANT,
    
    -- Sync tracking
    sync_id                 VARCHAR(100),
    sync_status             VARCHAR(30),
    synced_to               VARCHAR(50),
    synced_at               TIMESTAMP_NTZ,
    
    -- Metadata
    detected_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    detected_by             VARCHAR(100),
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 5. SYNC METRICS TABLE - Performance tracking
-- ============================================================
CREATE OR REPLACE TABLE SYNC_METRICS (
    metric_id               NUMBER AUTOINCREMENT PRIMARY KEY,
    metric_timestamp        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    metric_name             VARCHAR(200) NOT NULL,
    metric_value            FLOAT NOT NULL,
    metric_unit             VARCHAR(50),
    
    -- Context
    sync_id                 VARCHAR(100),
    sync_direction          VARCHAR(50),
    model_name              VARCHAR(500),
    
    -- Dimensions
    dimension_1_name        VARCHAR(100),
    dimension_1_value       VARCHAR(500),
    dimension_2_name        VARCHAR(100),
    dimension_2_value       VARCHAR(500),
    
    -- Metadata
    environment             VARCHAR(50) DEFAULT 'PRODUCTION',
    collected_by            VARCHAR(100),
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 6. ERROR LOG TABLE - Detailed error tracking
-- ============================================================
CREATE OR REPLACE TABLE SYNC_ERRORS (
    error_id                NUMBER AUTOINCREMENT PRIMARY KEY,
    error_timestamp         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    sync_id                 VARCHAR(100),
    
    -- Error classification
    error_category          VARCHAR(100),  -- 'AUTHENTICATION', 'CONNECTION', 'VALIDATION', 'TRANSFORMATION', 'EXECUTION'
    error_severity          VARCHAR(30),   -- 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'
    error_code              VARCHAR(100),
    error_message           VARCHAR(4000),
    
    -- Stack trace and details
    stack_trace             VARCHAR(16000),
    error_context           VARIANT,
    
    -- Source information
    source_system           VARCHAR(50),
    source_operation        VARCHAR(200),
    source_model            VARCHAR(500),
    source_item             VARCHAR(500),
    
    -- Resolution tracking
    is_resolved             BOOLEAN DEFAULT FALSE,
    resolved_at             TIMESTAMP_NTZ,
    resolved_by             VARCHAR(100),
    resolution_notes        VARCHAR(4000),
    
    -- Notification tracking
    notification_sent       BOOLEAN DEFAULT FALSE,
    notification_sent_at    TIMESTAMP_NTZ,
    notification_channel    VARCHAR(100),
    
    -- Metadata
    environment             VARCHAR(50) DEFAULT 'PRODUCTION',
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 7. NOTIFICATION LOG TABLE
-- ============================================================
CREATE OR REPLACE TABLE NOTIFICATION_LOG (
    notification_id         NUMBER AUTOINCREMENT PRIMARY KEY,
    notification_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    notification_type       VARCHAR(100) NOT NULL,  -- 'SYNC_FAILURE', 'SYNC_SUCCESS', 'HEALTH_CHECK', 'ALERT'
    notification_channel    VARCHAR(50) NOT NULL,   -- 'EMAIL', 'SLACK', 'TEAMS', 'WEBHOOK'
    
    -- Recipients
    recipients              ARRAY,
    
    -- Content
    subject                 VARCHAR(500),
    message_body            VARCHAR(16000),
    message_data            VARIANT,
    
    -- Delivery status
    delivery_status         VARCHAR(30),  -- 'PENDING', 'SENT', 'FAILED', 'DELIVERED'
    delivery_timestamp      TIMESTAMP_NTZ,
    delivery_error          VARCHAR(2000),
    retry_count             NUMBER DEFAULT 0,
    
    -- Context
    sync_id                 VARCHAR(100),
    error_id                NUMBER,
    
    -- Metadata
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- 8. CREATE INDEXES FOR PERFORMANCE
-- ============================================================
CREATE OR REPLACE INDEX idx_audit_sync_id ON SYNC_AUDIT_LOG(sync_id);
CREATE OR REPLACE INDEX idx_audit_timestamp ON SYNC_AUDIT_LOG(sync_timestamp);
CREATE OR REPLACE INDEX idx_audit_status ON SYNC_AUDIT_LOG(sync_status);

CREATE OR REPLACE INDEX idx_changes_model ON MODEL_CHANGE_HISTORY(model_name, detected_at);
CREATE OR REPLACE INDEX idx_changes_sync ON MODEL_CHANGE_HISTORY(sync_id);

CREATE OR REPLACE INDEX idx_metrics_timestamp ON SYNC_METRICS(metric_timestamp, metric_name);
CREATE OR REPLACE INDEX idx_metrics_sync ON SYNC_METRICS(sync_id);

CREATE OR REPLACE INDEX idx_errors_timestamp ON SYNC_ERRORS(error_timestamp);
CREATE OR REPLACE INDEX idx_errors_category ON SYNC_ERRORS(error_category, error_severity);
CREATE OR REPLACE INDEX idx_errors_unresolved ON SYNC_ERRORS(is_resolved, error_timestamp);

-- ============================================================
-- 9. CREATE VIEWS FOR MONITORING
-- ============================================================

-- Latest sync status view
CREATE OR REPLACE VIEW VW_LATEST_SYNC_STATUS AS
SELECT 
    ss.sync_type,
    ss.last_successful_sync,
    ss.last_attempted_sync,
    ss.last_sync_status,
    ss.consecutive_failures,
    ss.is_locked,
    TIMESTAMPDIFF('MINUTE', ss.last_successful_sync, CURRENT_TIMESTAMP()) AS minutes_since_last_success,
    CASE 
        WHEN ss.consecutive_failures >= 3 THEN 'CRITICAL'
        WHEN ss.consecutive_failures >= 1 THEN 'WARNING'
        WHEN TIMESTAMPDIFF('MINUTE', ss.last_successful_sync, CURRENT_TIMESTAMP()) > 120 THEN 'STALE'
        ELSE 'HEALTHY'
    END AS health_status
FROM SYNC_STATE ss;

-- Sync performance summary view
CREATE OR REPLACE VIEW VW_SYNC_PERFORMANCE_24H AS
SELECT 
    DATE_TRUNC('HOUR', sync_timestamp) AS hour_bucket,
    sync_direction,
    COUNT(*) AS sync_count,
    SUM(CASE WHEN sync_status = 'COMPLETED' THEN 1 ELSE 0 END) AS successful_count,
    SUM(CASE WHEN sync_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    AVG(execution_duration_ms) AS avg_duration_ms,
    MAX(execution_duration_ms) AS max_duration_ms,
    SUM(models_processed) AS total_models_processed,
    SUM(views_created) AS total_views_created,
    SUM(measures_synced) AS total_measures_synced
FROM SYNC_AUDIT_LOG
WHERE sync_timestamp >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- Unresolved errors view
CREATE OR REPLACE VIEW VW_UNRESOLVED_ERRORS AS
SELECT 
    error_id,
    error_timestamp,
    error_category,
    error_severity,
    error_code,
    error_message,
    source_system,
    source_model,
    sync_id,
    TIMESTAMPDIFF('MINUTE', error_timestamp, CURRENT_TIMESTAMP()) AS minutes_ago
FROM SYNC_ERRORS
WHERE is_resolved = FALSE
ORDER BY 
    CASE error_severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    error_timestamp DESC;

-- Daily sync summary view
CREATE OR REPLACE VIEW VW_DAILY_SYNC_SUMMARY AS
SELECT 
    DATE(sync_timestamp) AS sync_date,
    sync_direction,
    COUNT(*) AS total_syncs,
    SUM(CASE WHEN sync_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN sync_status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN sync_status = 'PARTIAL' THEN 1 ELSE 0 END) AS partial,
    ROUND(100.0 * SUM(CASE WHEN sync_status = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_rate_pct,
    AVG(execution_duration_ms) AS avg_duration_ms,
    SUM(models_processed) AS total_models,
    SUM(views_created + views_updated) AS total_changes
FROM SYNC_AUDIT_LOG
WHERE sync_timestamp >= DATEADD('DAY', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

COMMENT ON TABLE SYNC_AUDIT_LOG IS 'Comprehensive audit log for all Fabric-Snowflake sync operations';
COMMENT ON TABLE SYNC_STATE IS 'Current state tracking for sync operations with distributed locking support';
COMMENT ON TABLE MODEL_CHANGE_HISTORY IS 'History of all detected changes in semantic models';
COMMENT ON TABLE SYNC_METRICS IS 'Performance and operational metrics for sync operations';
COMMENT ON TABLE SYNC_ERRORS IS 'Detailed error tracking with resolution workflow';
COMMENT ON TABLE NOTIFICATION_LOG IS 'Log of all notifications sent for sync events';
