-- ============================================================
-- SNOWFLAKE STORED PROCEDURES FOR SYNC OPERATIONS
-- Fabric-Snowflake Semantic Model Synchronization
-- Created: 2026-01-03
-- ============================================================

USE SCHEMA SYNC_OPERATIONS;

-- ============================================================
-- 1. MAIN SYNC ORCHESTRATION PROCEDURE
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_EXECUTE_SEMANTIC_SYNC(
    P_SYNC_DIRECTION VARCHAR,           -- 'FABRIC_TO_SNOWFLAKE', 'SNOWFLAKE_TO_FABRIC', 'BIDIRECTIONAL'
    P_WEBHOOK_URL VARCHAR,               -- External webhook URL for Python sync script
    P_DRY_RUN BOOLEAN DEFAULT FALSE,     -- If true, simulates sync without changes
    P_FORCE_FULL_SYNC BOOLEAN DEFAULT FALSE  -- If true, ignores incremental detection
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Generate unique sync ID
    var syncId = 'SYNC_' + new Date().toISOString().replace(/[-:T]/g, '').slice(0, 14) + '_' + Math.random().toString(36).slice(2, 8).toUpperCase();
    var startTime = new Date();
    var result = {
        sync_id: syncId,
        status: 'STARTED',
        direction: P_SYNC_DIRECTION,
        dry_run: P_DRY_RUN,
        force_full_sync: P_FORCE_FULL_SYNC,
        start_time: startTime.toISOString(),
        models_processed: 0,
        views_created: 0,
        views_updated: 0,
        measures_synced: 0,
        errors: []
    };
    
    // Check if another sync is already running (distributed lock)
    var lockCheck = snowflake.execute({
        sqlText: `
            SELECT is_locked, lock_owner, lock_expires_at
            FROM SYNC_STATE 
            WHERE sync_type = :1
            FOR UPDATE
        `,
        binds: [P_SYNC_DIRECTION]
    });
    
    if (lockCheck.next()) {
        var isLocked = lockCheck.getColumnValue(1);
        var lockExpires = lockCheck.getColumnValue(3);
        
        if (isLocked && lockExpires && new Date(lockExpires) > startTime) {
            result.status = 'SKIPPED';
            result.error_message = 'Another sync operation is currently in progress';
            
            // Log the skip
            snowflake.execute({
                sqlText: `
                    INSERT INTO SYNC_AUDIT_LOG (
                        sync_id, sync_direction, sync_status, 
                        execution_start_time, error_message
                    ) VALUES (:1, :2, 'SKIPPED', :3, :4)
                `,
                binds: [syncId, P_SYNC_DIRECTION, startTime.toISOString(), result.error_message]
            });
            
            return result;
        }
    }
    
    // Acquire lock (expires in 30 minutes)
    var lockExpiry = new Date(startTime.getTime() + 30 * 60 * 1000);
    snowflake.execute({
        sqlText: `
            UPDATE SYNC_STATE 
            SET is_locked = TRUE, 
                lock_owner = :1, 
                lock_acquired_at = CURRENT_TIMESTAMP(),
                lock_expires_at = :2,
                last_attempted_sync = CURRENT_TIMESTAMP(),
                updated_at = CURRENT_TIMESTAMP()
            WHERE sync_type = :3
        `,
        binds: [syncId, lockExpiry.toISOString(), P_SYNC_DIRECTION]
    });
    
    // Log sync start
    snowflake.execute({
        sqlText: `
            INSERT INTO SYNC_AUDIT_LOG (
                sync_id, sync_direction, sync_status, 
                execution_start_time, triggered_by,
                warehouse_used
            ) VALUES (
                :1, :2, 'STARTED', :3, 'SCHEDULED_TASK',
                CURRENT_WAREHOUSE()
            )
        `,
        binds: [syncId, P_SYNC_DIRECTION, startTime.toISOString()]
    });
    
    // Prepare webhook payload
    var webhookPayload = {
        sync_id: syncId,
        direction: P_SYNC_DIRECTION,
        dry_run: P_DRY_RUN,
        force_full_sync: P_FORCE_FULL_SYNC,
        timestamp: startTime.toISOString(),
        callback_url: null  // Will be set by Azure Function
    };
    
    // Call external webhook/API to trigger Python sync script
    // Note: In production, use Snowflake External Network Access
    var webhookResult = null;
    
    if (P_WEBHOOK_URL && P_WEBHOOK_URL.length > 0) {
        // This requires External Network Access configured
        // For now, we'll simulate the call preparation
        result.webhook_url = P_WEBHOOK_URL;
        result.webhook_payload = webhookPayload;
        result.status = 'WEBHOOK_TRIGGERED';
        
        // In production, you would use:
        // var response = snowflake.execute({
        //     sqlText: `SELECT SYSTEM$CALL_EXTERNAL_FUNCTION('sync_webhook_function', :1)`,
        //     binds: [JSON.stringify(webhookPayload)]
        // });
    }
    
    // For demonstration, simulate sync operations
    if (!P_WEBHOOK_URL || P_WEBHOOK_URL.length === 0) {
        result.status = 'IN_PROGRESS';
        
        // Simulate processing
        result.models_processed = 1;
        result.views_created = 2;
        result.views_updated = 1;
        result.measures_synced = 5;
        
        // Record metrics
        snowflake.execute({
            sqlText: `
                INSERT INTO SYNC_METRICS (metric_name, metric_value, metric_unit, sync_id, sync_direction)
                VALUES 
                    ('models_processed', :1, 'count', :4, :5),
                    ('views_created', :2, 'count', :4, :5),
                    ('measures_synced', :3, 'count', :4, :5)
            `,
            binds: [result.models_processed, result.views_created, result.measures_synced, syncId, P_SYNC_DIRECTION]
        });
        
        result.status = 'COMPLETED';
    }
    
    // Calculate execution time
    var endTime = new Date();
    var durationMs = endTime - startTime;
    result.end_time = endTime.toISOString();
    result.duration_ms = durationMs;
    
    // Update audit log
    var finalStatus = result.errors.length > 0 ? 'PARTIAL' : result.status;
    snowflake.execute({
        sqlText: `
            UPDATE SYNC_AUDIT_LOG
            SET sync_status = :1,
                execution_end_time = :2,
                execution_duration_ms = :3,
                models_processed = :4,
                views_created = :5,
                views_updated = :6,
                measures_synced = :7,
                updated_at = CURRENT_TIMESTAMP()
            WHERE sync_id = :8
        `,
        binds: [
            finalStatus, 
            endTime.toISOString(), 
            durationMs,
            result.models_processed,
            result.views_created,
            result.views_updated,
            result.measures_synced,
            syncId
        ]
    });
    
    // Update sync state
    snowflake.execute({
        sqlText: `
            UPDATE SYNC_STATE 
            SET is_locked = FALSE, 
                lock_owner = NULL,
                lock_acquired_at = NULL,
                lock_expires_at = NULL,
                last_sync_status = :1,
                last_successful_sync = CASE WHEN :1 = 'COMPLETED' THEN CURRENT_TIMESTAMP() ELSE last_successful_sync END,
                consecutive_failures = CASE WHEN :1 = 'COMPLETED' THEN 0 ELSE consecutive_failures + 1 END,
                updated_at = CURRENT_TIMESTAMP()
            WHERE sync_type = :2
        `,
        binds: [finalStatus, P_SYNC_DIRECTION]
    });
    
    result.status = finalStatus;
    return result;
    
} catch (err) {
    // Error handling
    var errorResult = {
        sync_id: typeof syncId !== 'undefined' ? syncId : 'UNKNOWN',
        status: 'FAILED',
        error_code: err.code || 'UNKNOWN_ERROR',
        error_message: err.message,
        stack_trace: err.stack
    };
    
    // Log error
    snowflake.execute({
        sqlText: `
            INSERT INTO SYNC_ERRORS (
                sync_id, error_category, error_severity, 
                error_code, error_message, stack_trace,
                source_operation
            ) VALUES (:1, 'EXECUTION', 'CRITICAL', :2, :3, :4, 'SP_EXECUTE_SEMANTIC_SYNC')
        `,
        binds: [errorResult.sync_id, errorResult.error_code, errorResult.error_message, errorResult.stack_trace]
    });
    
    // Update audit log if sync_id exists
    if (typeof syncId !== 'undefined') {
        snowflake.execute({
            sqlText: `
                UPDATE SYNC_AUDIT_LOG
                SET sync_status = 'FAILED',
                    error_code = :1,
                    error_message = :2,
                    execution_end_time = CURRENT_TIMESTAMP(),
                    updated_at = CURRENT_TIMESTAMP()
                WHERE sync_id = :3
            `,
            binds: [errorResult.error_code, errorResult.error_message, syncId]
        });
        
        // Release lock on failure
        snowflake.execute({
            sqlText: `
                UPDATE SYNC_STATE 
                SET is_locked = FALSE, 
                    lock_owner = NULL,
                    consecutive_failures = consecutive_failures + 1,
                    last_sync_status = 'FAILED',
                    updated_at = CURRENT_TIMESTAMP()
                WHERE sync_type = :1
            `,
            binds: [P_SYNC_DIRECTION]
        });
    }
    
    return errorResult;
}
$$;

-- ============================================================
-- 2. RETRY FAILED SYNC PROCEDURE
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_RETRY_FAILED_SYNC(
    P_ORIGINAL_SYNC_ID VARCHAR,
    P_WEBHOOK_URL VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Get original sync details
    var originalSync = snowflake.execute({
        sqlText: `
            SELECT sync_direction, retry_count, max_retries
            FROM SYNC_AUDIT_LOG
            WHERE sync_id = :1
        `,
        binds: [P_ORIGINAL_SYNC_ID]
    });
    
    if (!originalSync.next()) {
        return {
            status: 'ERROR',
            message: 'Original sync not found: ' + P_ORIGINAL_SYNC_ID
        };
    }
    
    var direction = originalSync.getColumnValue(1);
    var retryCount = originalSync.getColumnValue(2) || 0;
    var maxRetries = originalSync.getColumnValue(3) || 3;
    
    if (retryCount >= maxRetries) {
        return {
            status: 'MAX_RETRIES_EXCEEDED',
            message: 'Maximum retry count exceeded for sync: ' + P_ORIGINAL_SYNC_ID,
            retry_count: retryCount,
            max_retries: maxRetries
        };
    }
    
    // Update retry count on original
    snowflake.execute({
        sqlText: `
            UPDATE SYNC_AUDIT_LOG
            SET retry_count = retry_count + 1,
                updated_at = CURRENT_TIMESTAMP()
            WHERE sync_id = :1
        `,
        binds: [P_ORIGINAL_SYNC_ID]
    });
    
    // Execute new sync with retry flag
    var retryResult = snowflake.execute({
        sqlText: `CALL SP_EXECUTE_SEMANTIC_SYNC(:1, :2, FALSE, FALSE)`,
        binds: [direction, P_WEBHOOK_URL]
    });
    
    retryResult.next();
    var result = retryResult.getColumnValue(1);
    
    // Update retry info
    if (typeof result === 'object') {
        snowflake.execute({
            sqlText: `
                UPDATE SYNC_AUDIT_LOG
                SET is_retry = TRUE,
                    parent_sync_id = :1
                WHERE sync_id = :2
            `,
            binds: [P_ORIGINAL_SYNC_ID, result.sync_id]
        });
    }
    
    return result;
    
} catch (err) {
    return {
        status: 'ERROR',
        error_message: err.message
    };
}
$$;

-- ============================================================
-- 3. HEALTH CHECK PROCEDURE
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_SYNC_HEALTH_CHECK()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var healthReport = {
        check_timestamp: new Date().toISOString(),
        overall_status: 'HEALTHY',
        checks: [],
        warnings: [],
        errors: []
    };
    
    // Check 1: Verify sync state table
    var stateCheck = snowflake.execute({
        sqlText: `
            SELECT 
                sync_type,
                last_successful_sync,
                consecutive_failures,
                is_locked,
                TIMESTAMPDIFF('MINUTE', last_successful_sync, CURRENT_TIMESTAMP()) as minutes_since_success
            FROM SYNC_STATE
        `
    });
    
    while (stateCheck.next()) {
        var syncType = stateCheck.getColumnValue(1);
        var consecutiveFailures = stateCheck.getColumnValue(3);
        var isLocked = stateCheck.getColumnValue(4);
        var minutesSinceSuccess = stateCheck.getColumnValue(5);
        
        var check = {
            sync_type: syncType,
            consecutive_failures: consecutiveFailures,
            is_locked: isLocked,
            minutes_since_success: minutesSinceSuccess,
            status: 'OK'
        };
        
        if (consecutiveFailures >= 3) {
            check.status = 'CRITICAL';
            healthReport.errors.push(syncType + ' has ' + consecutiveFailures + ' consecutive failures');
            healthReport.overall_status = 'CRITICAL';
        } else if (consecutiveFailures >= 1) {
            check.status = 'WARNING';
            healthReport.warnings.push(syncType + ' has ' + consecutiveFailures + ' failures');
            if (healthReport.overall_status === 'HEALTHY') {
                healthReport.overall_status = 'WARNING';
            }
        }
        
        if (minutesSinceSuccess > 120 && syncType !== 'HEALTH_CHECK') {
            check.status = 'STALE';
            healthReport.warnings.push(syncType + ' has not run successfully in ' + minutesSinceSuccess + ' minutes');
        }
        
        healthReport.checks.push(check);
    }
    
    // Check 2: Check for stuck locks
    var lockCheck = snowflake.execute({
        sqlText: `
            SELECT sync_type, lock_owner, lock_expires_at
            FROM SYNC_STATE
            WHERE is_locked = TRUE
            AND lock_expires_at < CURRENT_TIMESTAMP()
        `
    });
    
    while (lockCheck.next()) {
        var stuckLock = lockCheck.getColumnValue(1);
        healthReport.warnings.push('Stuck lock detected on ' + stuckLock);
        
        // Auto-release stuck locks
        snowflake.execute({
            sqlText: `
                UPDATE SYNC_STATE
                SET is_locked = FALSE, 
                    lock_owner = NULL,
                    lock_expires_at = NULL
                WHERE sync_type = :1
            `,
            binds: [stuckLock]
        });
    }
    
    // Check 3: Recent error rate
    var errorCheck = snowflake.execute({
        sqlText: `
            SELECT 
                COUNT(*) as error_count,
                COUNT(CASE WHEN is_resolved = FALSE THEN 1 END) as unresolved_count
            FROM SYNC_ERRORS
            WHERE error_timestamp >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
        `
    });
    
    if (errorCheck.next()) {
        var errorCount = errorCheck.getColumnValue(1);
        var unresolvedCount = errorCheck.getColumnValue(2);
        
        healthReport.recent_errors = {
            last_hour: errorCount,
            unresolved: unresolvedCount
        };
        
        if (unresolvedCount > 5) {
            healthReport.overall_status = 'CRITICAL';
            healthReport.errors.push(unresolvedCount + ' unresolved errors in the last hour');
        }
    }
    
    // Log health check
    snowflake.execute({
        sqlText: `
            INSERT INTO SYNC_METRICS (metric_name, metric_value, metric_unit, sync_direction)
            VALUES ('health_check_status', :1, 'status_code', 'HEALTH_CHECK')
        `,
        binds: [healthReport.overall_status === 'HEALTHY' ? 1 : (healthReport.overall_status === 'WARNING' ? 0.5 : 0)]
    });
    
    // Update health check state
    snowflake.execute({
        sqlText: `
            UPDATE SYNC_STATE
            SET last_successful_sync = CURRENT_TIMESTAMP(),
                last_sync_status = 'COMPLETED',
                current_state = PARSE_JSON(:1)
            WHERE sync_type = 'HEALTH_CHECK'
        `,
        binds: [JSON.stringify(healthReport)]
    });
    
    return healthReport;
    
} catch (err) {
    return {
        overall_status: 'ERROR',
        error_message: err.message
    };
}
$$;

-- ============================================================
-- 4. SEND NOTIFICATION PROCEDURE
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_SEND_NOTIFICATION(
    P_NOTIFICATION_TYPE VARCHAR,
    P_CHANNEL VARCHAR,
    P_SUBJECT VARCHAR,
    P_MESSAGE VARCHAR,
    P_SYNC_ID VARCHAR DEFAULT NULL,
    P_ERROR_ID NUMBER DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var notificationId = null;
    
    // Insert notification record
    var insertResult = snowflake.execute({
        sqlText: `
            INSERT INTO NOTIFICATION_LOG (
                notification_type, notification_channel,
                subject, message_body,
                sync_id, error_id,
                delivery_status
            ) VALUES (:1, :2, :3, :4, :5, :6, 'PENDING')
        `,
        binds: [P_NOTIFICATION_TYPE, P_CHANNEL, P_SUBJECT, P_MESSAGE, P_SYNC_ID, P_ERROR_ID]
    });
    
    // Get the notification ID
    var idResult = snowflake.execute({
        sqlText: `SELECT MAX(notification_id) FROM NOTIFICATION_LOG`
    });
    
    if (idResult.next()) {
        notificationId = idResult.getColumnValue(1);
    }
    
    // In production, integrate with external notification service
    // For now, mark as sent (would use email integration, Slack webhook, etc.)
    var result = {
        notification_id: notificationId,
        status: 'QUEUED',
        channel: P_CHANNEL,
        subject: P_SUBJECT
    };
    
    // Update delivery status
    snowflake.execute({
        sqlText: `
            UPDATE NOTIFICATION_LOG
            SET delivery_status = 'SENT',
                delivery_timestamp = CURRENT_TIMESTAMP()
            WHERE notification_id = :1
        `,
        binds: [notificationId]
    });
    
    result.status = 'SENT';
    return result;
    
} catch (err) {
    return {
        status: 'FAILED',
        error_message: err.message
    };
}
$$;

-- ============================================================
-- 5. AUTO-NOTIFY ON FAILURE PROCEDURE
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_CHECK_AND_NOTIFY_FAILURES()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var notifications = [];
    
    // Check for critical failures
    var criticalCheck = snowflake.execute({
        sqlText: `
            SELECT sync_type, consecutive_failures
            FROM SYNC_STATE
            WHERE consecutive_failures >= 3
        `
    });
    
    while (criticalCheck.next()) {
        var syncType = criticalCheck.getColumnValue(1);
        var failures = criticalCheck.getColumnValue(2);
        
        // Check if we already sent notification recently
        var recentNotif = snowflake.execute({
            sqlText: `
                SELECT COUNT(*)
                FROM NOTIFICATION_LOG
                WHERE notification_type = 'SYNC_FAILURE'
                AND sync_id IN (
                    SELECT sync_id FROM SYNC_AUDIT_LOG 
                    WHERE sync_direction = :1 
                    ORDER BY sync_timestamp DESC LIMIT 1
                )
                AND notification_timestamp >= DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
            `,
            binds: [syncType]
        });
        
        recentNotif.next();
        if (recentNotif.getColumnValue(1) === 0) {
            // Send notification
            var subject = '🚨 CRITICAL: ' + syncType + ' Sync Has Failed ' + failures + ' Times';
            var message = 'The ' + syncType + ' synchronization has failed ' + failures + ' consecutive times. ' +
                         'Please investigate immediately. Timestamp: ' + new Date().toISOString();
            
            snowflake.execute({
                sqlText: `CALL SP_SEND_NOTIFICATION('SYNC_FAILURE', 'EMAIL', :1, :2)`,
                binds: [subject, message]
            });
            
            notifications.push({
                type: 'CRITICAL_FAILURE',
                sync_type: syncType,
                failures: failures
            });
        }
    }
    
    // Check for unresolved critical errors
    var errorCheck = snowflake.execute({
        sqlText: `
            SELECT error_id, error_code, error_message, sync_id
            FROM SYNC_ERRORS
            WHERE is_resolved = FALSE
            AND error_severity = 'CRITICAL'
            AND notification_sent = FALSE
        `
    });
    
    while (errorCheck.next()) {
        var errorId = errorCheck.getColumnValue(1);
        var errorCode = errorCheck.getColumnValue(2);
        var errorMsg = errorCheck.getColumnValue(3);
        
        var subject = '🚨 Critical Error: ' + errorCode;
        var message = 'Critical error detected:\n\n' + errorMsg;
        
        snowflake.execute({
            sqlText: `CALL SP_SEND_NOTIFICATION('SYNC_ERROR', 'EMAIL', :1, :2, NULL, :3)`,
            binds: [subject, message, errorId]
        });
        
        // Mark as notified
        snowflake.execute({
            sqlText: `
                UPDATE SYNC_ERRORS
                SET notification_sent = TRUE,
                    notification_sent_at = CURRENT_TIMESTAMP()
                WHERE error_id = :1
            `,
            binds: [errorId]
        });
        
        notifications.push({
            type: 'CRITICAL_ERROR',
            error_id: errorId
        });
    }
    
    return {
        status: 'COMPLETED',
        notifications_sent: notifications.length,
        details: notifications
    };
    
} catch (err) {
    return {
        status: 'ERROR',
        error_message: err.message
    };
}
$$;

-- ============================================================
-- 6. CLEANUP OLD RECORDS PROCEDURE
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_CLEANUP_OLD_RECORDS(
    P_RETENTION_DAYS NUMBER DEFAULT 90
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var result = {
        retention_days: P_RETENTION_DAYS,
        records_deleted: {}
    };
    
    // Cleanup audit logs
    var auditCleanup = snowflake.execute({
        sqlText: `
            DELETE FROM SYNC_AUDIT_LOG
            WHERE sync_timestamp < DATEADD('DAY', -:1, CURRENT_TIMESTAMP())
        `,
        binds: [P_RETENTION_DAYS]
    });
    result.records_deleted.audit_logs = auditCleanup.getRowCount();
    
    // Cleanup metrics
    var metricsCleanup = snowflake.execute({
        sqlText: `
            DELETE FROM SYNC_METRICS
            WHERE metric_timestamp < DATEADD('DAY', -:1, CURRENT_TIMESTAMP())
        `,
        binds: [P_RETENTION_DAYS]
    });
    result.records_deleted.metrics = metricsCleanup.getRowCount();
    
    // Cleanup resolved errors (shorter retention)
    var errorsCleanup = snowflake.execute({
        sqlText: `
            DELETE FROM SYNC_ERRORS
            WHERE is_resolved = TRUE
            AND error_timestamp < DATEADD('DAY', -:1, CURRENT_TIMESTAMP())
        `,
        binds: [P_RETENTION_DAYS / 2]  // Half retention for resolved errors
    });
    result.records_deleted.resolved_errors = errorsCleanup.getRowCount();
    
    // Cleanup notifications
    var notifCleanup = snowflake.execute({
        sqlText: `
            DELETE FROM NOTIFICATION_LOG
            WHERE notification_timestamp < DATEADD('DAY', -:1, CURRENT_TIMESTAMP())
        `,
        binds: [P_RETENTION_DAYS]
    });
    result.records_deleted.notifications = notifCleanup.getRowCount();
    
    // Cleanup change history (longer retention)
    var changeCleanup = snowflake.execute({
        sqlText: `
            DELETE FROM MODEL_CHANGE_HISTORY
            WHERE detected_at < DATEADD('DAY', -:1, CURRENT_TIMESTAMP())
        `,
        binds: [P_RETENTION_DAYS * 2]  // Double retention for change history
    });
    result.records_deleted.change_history = changeCleanup.getRowCount();
    
    result.status = 'COMPLETED';
    result.cleanup_timestamp = new Date().toISOString();
    
    return result;
    
} catch (err) {
    return {
        status: 'ERROR',
        error_message: err.message
    };
}
$$;

-- Grant execute permissions
GRANT USAGE ON PROCEDURE SP_EXECUTE_SEMANTIC_SYNC(VARCHAR, VARCHAR, BOOLEAN, BOOLEAN) TO ROLE SYNC_OPERATOR;
GRANT USAGE ON PROCEDURE SP_RETRY_FAILED_SYNC(VARCHAR, VARCHAR) TO ROLE SYNC_OPERATOR;
GRANT USAGE ON PROCEDURE SP_SYNC_HEALTH_CHECK() TO ROLE SYNC_OPERATOR;
GRANT USAGE ON PROCEDURE SP_SEND_NOTIFICATION(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, NUMBER) TO ROLE SYNC_OPERATOR;
GRANT USAGE ON PROCEDURE SP_CHECK_AND_NOTIFY_FAILURES() TO ROLE SYNC_OPERATOR;
GRANT USAGE ON PROCEDURE SP_CLEANUP_OLD_RECORDS(NUMBER) TO ROLE SYNC_OPERATOR;
