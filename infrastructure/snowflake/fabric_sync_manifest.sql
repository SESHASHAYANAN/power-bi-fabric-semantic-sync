-- ============================================================================
-- SYNC MANIFEST TABLE - Fabric Side
-- ============================================================================
-- This table tracks all sync operations from Fabric's perspective.
-- Every file/table synced to or from Fabric has an entry here.
--
-- Usage: Create this table in your Fabric Lakehouse SQL endpoint
-- ============================================================================

CREATE TABLE IF NOT EXISTS fabric_sync_manifest (
    -- Primary identifier (UUID v4)
    sync_id                    VARCHAR(36) NOT NULL PRIMARY KEY,
    
    -- Source/Target information
    source_table               VARCHAR(255) NOT NULL,
    target_table               VARCHAR(255),
    filename                   VARCHAR(500),
    source_platform            VARCHAR(50) NOT NULL DEFAULT 'fabric',  -- fabric|snowflake|file_upload
    target_platform            VARCHAR(50) NOT NULL DEFAULT 'snowflake', -- fabric|snowflake|both
    
    -- Status tracking
    status                     VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING|SYNCING|SYNCED|FAILED|CONFLICT|ROLLBACK|RETRY_PENDING
    
    -- Timestamps
    created_at                 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by                 VARCHAR(100) NOT NULL DEFAULT 'system',
    synced_at                  TIMESTAMP,
    last_modified_at           TIMESTAMP,
    
    -- Data integrity
    row_count_source           BIGINT,
    row_count_target           BIGINT,
    schema_hash                VARCHAR(64),      -- SHA256 of schema definition
    data_hash                  VARCHAR(64),      -- SHA256 of data content
    
    -- Error tracking
    error_message              TEXT,
    retry_count                INT DEFAULT 0,
    
    -- Migration tracking
    migrated                   BOOLEAN DEFAULT FALSE,
    migrated_at                TIMESTAMP,
    
    -- Versioning for conflict detection
    sync_version               INT DEFAULT 1,
    
    -- Additional metadata as JSON
    metadata                   VARCHAR(MAX)  -- JSON object
);

-- Indexes for common queries
CREATE INDEX idx_fabric_sync_status ON fabric_sync_manifest(status);
CREATE INDEX idx_fabric_sync_created ON fabric_sync_manifest(created_at DESC);
CREATE INDEX idx_fabric_sync_source ON fabric_sync_manifest(source_table);
CREATE INDEX idx_fabric_sync_target ON fabric_sync_manifest(target_table);

-- ============================================================================
-- COMMENTS
-- ============================================================================

-- sync_id: UUID v4 generated at sync start. This is the idempotency key.
--          Before any sync, check: SELECT COUNT(*) WHERE sync_id = ?
--          If exists and status = 'SYNCED', skip the operation.

-- schema_hash: Hash of column definitions (name, type, nullable).
--              Used to detect schema drift between platforms.

-- data_hash: SHA256 of sorted, serialized data.
--            Must match between source and target after sync.

-- sync_version: Incremented on each update. Used for optimistic locking
--               and conflict detection with concurrent modifications.
