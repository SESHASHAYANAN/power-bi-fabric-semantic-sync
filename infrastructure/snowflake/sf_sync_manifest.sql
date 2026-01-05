-- ============================================================================
-- SYNC MANIFEST TABLE - Snowflake Side
-- ============================================================================
-- This table tracks all sync operations from Snowflake's perspective.
-- Every file/table synced to or from Snowflake has an entry here.
--
-- Usage: Execute in your Snowflake database
-- ============================================================================

CREATE TABLE IF NOT EXISTS SF_SYNC_MANIFEST (
    -- Primary identifier (UUID v4)
    SYNC_ID                    VARCHAR(36) NOT NULL PRIMARY KEY,
    
    -- Source/Target information
    SOURCE_TABLE               VARCHAR(255) NOT NULL,
    TARGET_TABLE               VARCHAR(255),
    FILENAME                   VARCHAR(500),
    SOURCE_PLATFORM            VARCHAR(50) NOT NULL DEFAULT 'fabric',  -- fabric|snowflake|file_upload
    TARGET_PLATFORM            VARCHAR(50) NOT NULL DEFAULT 'snowflake', -- fabric|snowflake|both
    
    -- Status tracking
    STATUS                     VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING|SYNCING|SYNCED|FAILED|CONFLICT|ROLLBACK|RETRY_PENDING
    
    -- Timestamps
    CREATED_AT                 TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY                 VARCHAR(100) NOT NULL DEFAULT 'system',
    SYNCED_AT                  TIMESTAMP_NTZ,
    LAST_MODIFIED_AT           TIMESTAMP_NTZ,
    
    -- Data integrity
    ROW_COUNT_SOURCE           NUMBER(19,0),
    ROW_COUNT_TARGET           NUMBER(19,0),
    SCHEMA_HASH                VARCHAR(64),      -- SHA256 of schema definition
    DATA_HASH                  VARCHAR(64),      -- SHA256 of data content
    
    -- Error tracking
    ERROR_MESSAGE              VARCHAR(16777216),
    RETRY_COUNT                NUMBER(10,0) DEFAULT 0,
    
    -- Migration tracking
    MIGRATED                   BOOLEAN DEFAULT FALSE,
    MIGRATED_AT                TIMESTAMP_NTZ,
    
    -- Versioning for conflict detection
    SYNC_VERSION               NUMBER(10,0) DEFAULT 1,
    
    -- Additional metadata as JSON
    METADATA                   VARIANT
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS IDX_SF_SYNC_STATUS ON SF_SYNC_MANIFEST(STATUS);
CREATE INDEX IF NOT EXISTS IDX_SF_SYNC_CREATED ON SF_SYNC_MANIFEST(CREATED_AT DESC);
CREATE INDEX IF NOT EXISTS IDX_SF_SYNC_SOURCE ON SF_SYNC_MANIFEST(SOURCE_TABLE);

-- ============================================================================
-- SYNC FAILURE QUEUE TABLE
-- ============================================================================
-- Failed operations pending retry with exponential backoff configuration.

CREATE TABLE IF NOT EXISTS SYNC_FAILURE_QUEUE (
    QUEUE_ID                   VARCHAR(36) NOT NULL PRIMARY KEY,
    SYNC_ID                    VARCHAR(36) NOT NULL,
    
    -- Error classification
    ERROR_TYPE                 VARCHAR(20),  -- TRANSIENT|VALIDATION|PERMISSION|DATA_CORRUPTION|UNKNOWN
    ERROR_MESSAGE              VARCHAR(16777216),
    
    -- Retry configuration
    RETRY_COUNT                NUMBER(10,0) DEFAULT 0,
    MAX_RETRIES                NUMBER(10,0) DEFAULT 5,
    NEXT_RETRY_AT              TIMESTAMP_NTZ,
    
    -- Timestamps
    FAILED_AT                  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    LAST_RETRY_AT              TIMESTAMP_NTZ,
    
    -- Context for retry operation
    OPERATION_CONTEXT          VARIANT,
    
    -- Foreign key reference
    CONSTRAINT FK_QUEUE_SYNC FOREIGN KEY (SYNC_ID) REFERENCES SF_SYNC_MANIFEST(SYNC_ID)
);

CREATE INDEX IF NOT EXISTS IDX_QUEUE_NEXT_RETRY ON SYNC_FAILURE_QUEUE(NEXT_RETRY_AT);
CREATE INDEX IF NOT EXISTS IDX_QUEUE_ERROR_TYPE ON SYNC_FAILURE_QUEUE(ERROR_TYPE);

-- ============================================================================
-- CONFLICT LOG TABLE
-- ============================================================================
-- Records all detected conflicts with both versions for audit and review.

CREATE TABLE IF NOT EXISTS CONFLICT_LOG (
    CONFLICT_ID                VARCHAR(36) NOT NULL PRIMARY KEY,
    SYNC_ID                    VARCHAR(36) NOT NULL,
    RECORD_ID                  VARCHAR(255) NOT NULL,
    
    -- Platform information
    SOURCE_PLATFORM            VARCHAR(50),
    TARGET_PLATFORM            VARCHAR(50),
    
    -- Version snapshots (both kept for audit)
    SOURCE_VERSION             VARIANT,
    TARGET_VERSION             VARIANT,
    SOURCE_TIMESTAMP           TIMESTAMP_NTZ,
    TARGET_TIMESTAMP           TIMESTAMP_NTZ,
    
    -- Resolution details
    RESOLVED_RECORD            VARIANT,
    RESOLUTION_METHOD          VARCHAR(30),  -- LAST_WRITE_WINS|PLATFORM_PRIORITY|MANUAL_REVIEW|SOURCE_WINS
    
    -- Audit timestamps
    DETECTED_AT                TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_AT                TIMESTAMP_NTZ,
    RESOLVED_BY                VARCHAR(100) DEFAULT 'system',
    
    -- Foreign key reference
    CONSTRAINT FK_CONFLICT_SYNC FOREIGN KEY (SYNC_ID) REFERENCES SF_SYNC_MANIFEST(SYNC_ID)
);

CREATE INDEX IF NOT EXISTS IDX_CONFLICT_SYNC ON CONFLICT_LOG(SYNC_ID);
CREATE INDEX IF NOT EXISTS IDX_CONFLICT_STATUS ON CONFLICT_LOG(RESOLVED_AT);

-- ============================================================================
-- AUDIT TRAIL TABLE
-- ============================================================================
-- Complete audit log for all sync operations.

CREATE TABLE IF NOT EXISTS SYNC_AUDIT_TRAIL (
    AUDIT_ID                   VARCHAR(36) NOT NULL PRIMARY KEY,
    SYNC_ID                    VARCHAR(36) NOT NULL,
    
    -- Operation details
    ACTION                     VARCHAR(50) NOT NULL,  -- DUAL_WRITE|SYNC_START|SYNC_COMPLETE|RETRY|CONFLICT_DETECTED|ROLLBACK|etc.
    ACTOR                      VARCHAR(100) DEFAULT 'system',  -- user_id or 'system'
    
    -- Context
    SOURCE_PLATFORM            VARCHAR(50),
    TARGET_PLATFORM            VARCHAR(50),
    AFFECTED_TABLE             VARCHAR(255),
    AFFECTED_ROWS              NUMBER(19,0),
    
    -- Status
    STATUS                     VARCHAR(20) DEFAULT 'INFO',  -- INFO|WARNING|ERROR|CRITICAL
    MESSAGE                    VARCHAR(16777216),
    
    -- Performance
    LATENCY_MS                 NUMBER(10,0),
    
    -- Timestamp
    TIMESTAMP                  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    
    -- Additional context as JSON
    CONTEXT                    VARIANT
);

CREATE INDEX IF NOT EXISTS IDX_AUDIT_SYNC ON SYNC_AUDIT_TRAIL(SYNC_ID);
CREATE INDEX IF NOT EXISTS IDX_AUDIT_ACTION ON SYNC_AUDIT_TRAIL(ACTION);
CREATE INDEX IF NOT EXISTS IDX_AUDIT_STATUS ON SYNC_AUDIT_TRAIL(STATUS);
CREATE INDEX IF NOT EXISTS IDX_AUDIT_TIME ON SYNC_AUDIT_TRAIL(TIMESTAMP DESC);

-- ============================================================================
-- HELPER VIEWS
-- ============================================================================

-- View: Recent sync failures requiring attention
CREATE OR REPLACE VIEW V_FAILED_SYNCS AS
SELECT 
    m.SYNC_ID,
    m.SOURCE_TABLE,
    m.TARGET_TABLE,
    m.STATUS,
    m.ERROR_MESSAGE,
    m.CREATED_AT,
    q.RETRY_COUNT,
    q.MAX_RETRIES,
    q.NEXT_RETRY_AT,
    q.ERROR_TYPE
FROM SF_SYNC_MANIFEST m
LEFT JOIN SYNC_FAILURE_QUEUE q ON m.SYNC_ID = q.SYNC_ID
WHERE m.STATUS IN ('FAILED', 'RETRY_PENDING')
ORDER BY m.CREATED_AT DESC;

-- View: Sync dashboard metrics
CREATE OR REPLACE VIEW V_SYNC_METRICS AS
SELECT 
    COUNT(*) AS TOTAL_SYNCS,
    SUM(CASE WHEN STATUS = 'SYNCED' THEN 1 ELSE 0 END) AS SUCCESSFUL_SYNCS,
    SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_SYNCS,
    SUM(CASE WHEN STATUS IN ('PENDING', 'SYNCING') THEN 1 ELSE 0 END) AS PENDING_SYNCS,
    ROUND((SUM(CASE WHEN STATUS = 'SYNCED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100, 2) AS SUCCESS_RATE_PCT,
    (SELECT COUNT(*) FROM SYNC_FAILURE_QUEUE WHERE NEXT_RETRY_AT > CURRENT_TIMESTAMP()) AS RETRY_QUEUE_SIZE,
    (SELECT COUNT(*) FROM CONFLICT_LOG WHERE RESOLVED_AT IS NULL) AS PENDING_CONFLICTS
FROM SF_SYNC_MANIFEST;

-- View: Recent audit events
CREATE OR REPLACE VIEW V_RECENT_AUDIT AS
SELECT 
    AUDIT_ID,
    SYNC_ID,
    ACTION,
    ACTOR,
    AFFECTED_TABLE,
    STATUS,
    MESSAGE,
    LATENCY_MS,
    TIMESTAMP
FROM SYNC_AUDIT_TRAIL
WHERE TIMESTAMP > DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC
LIMIT 1000;

-- ============================================================================
-- CLEANUP PROCEDURES
-- ============================================================================

-- Procedure: Clean old audit entries (keep 90 days)
CREATE OR REPLACE PROCEDURE SP_CLEANUP_OLD_AUDIT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM SYNC_AUDIT_TRAIL 
    WHERE TIMESTAMP < DATEADD(day, -90, CURRENT_TIMESTAMP());
    
    RETURN 'Cleanup complete';
END;
$$;

-- Procedure: Clean resolved conflicts older than 30 days
CREATE OR REPLACE PROCEDURE SP_CLEANUP_OLD_CONFLICTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    DELETE FROM CONFLICT_LOG 
    WHERE RESOLVED_AT IS NOT NULL 
    AND RESOLVED_AT < DATEADD(day, -30, CURRENT_TIMESTAMP());
    
    RETURN 'Cleanup complete';
END;
$$;

-- ============================================================================
-- GRANTS (adjust to your role structure)
-- ============================================================================

-- GRANT SELECT, INSERT, UPDATE, DELETE ON SF_SYNC_MANIFEST TO ROLE SYNC_SERVICE;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SYNC_FAILURE_QUEUE TO ROLE SYNC_SERVICE;
-- GRANT SELECT, INSERT ON CONFLICT_LOG TO ROLE SYNC_SERVICE;
-- GRANT SELECT, INSERT ON SYNC_AUDIT_TRAIL TO ROLE SYNC_SERVICE;
-- GRANT SELECT ON V_FAILED_SYNCS TO ROLE SYNC_ADMIN;
-- GRANT SELECT ON V_SYNC_METRICS TO ROLE SYNC_ADMIN;
-- GRANT SELECT ON V_RECENT_AUDIT TO ROLE SYNC_ADMIN;
