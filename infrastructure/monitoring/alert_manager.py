"""
Alert Manager for Fabric-Snowflake Sync
Handles all notifications and alerts across multiple channels

Author: Data Engineering Team
Created: 2026-01-03
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("AlertManager")


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertType(Enum):
    """Types of alerts."""
    SYNC_SUCCESS = "sync_success"
    SYNC_FAILURE = "sync_failure"
    SYNC_TIMEOUT = "sync_timeout"
    HEALTH_CHECK = "health_check"
    CONNECTION_ERROR = "connection_error"
    RATE_LIMIT = "rate_limit"
    SCHEMA_VALIDATION = "schema_validation"
    DATA_DRIFT = "data_drift"


@dataclass
class Alert:
    """Alert data structure."""
    alert_id: str
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sync_id: Optional[str] = None
    model_name: Optional[str] = None
    error_code: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "alert_type": self.alert_type.value,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "sync_id": self.sync_id,
            "model_name": self.model_name,
            "error_code": self.error_code,
            "metadata": self.metadata
        }


class AlertChannel(ABC):
    """Abstract base class for alert channels."""
    
    @abstractmethod
    def send(self, alert: Alert) -> bool:
        """Send alert through this channel."""
        pass
    
    @abstractmethod
    def is_configured(self) -> bool:
        """Check if channel is properly configured."""
        pass


class SlackAlertChannel(AlertChannel):
    """Slack webhook alert channel."""
    
    SEVERITY_COLORS = {
        AlertSeverity.INFO: "#2196F3",
        AlertSeverity.WARNING: "#FFC107",
        AlertSeverity.ERROR: "#FF5722",
        AlertSeverity.CRITICAL: "#F44336"
    }
    
    SEVERITY_EMOJIS = {
        AlertSeverity.INFO: "ℹ️",
        AlertSeverity.WARNING: "⚠️",
        AlertSeverity.ERROR: "🚨",
        AlertSeverity.CRITICAL: "🔥"
    }
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL", "")
    
    def is_configured(self) -> bool:
        return bool(self.webhook_url)
    
    def send(self, alert: Alert) -> bool:
        if not self.is_configured():
            logger.warning("Slack webhook not configured")
            return False
        
        try:
            payload = self._format_message(alert)
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Slack alert sent: {alert.alert_id}")
                return True
            else:
                logger.error(f"Slack send failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Slack alert error: {e}")
            return False
    
    def _format_message(self, alert: Alert) -> Dict[str, Any]:
        """Format alert as Slack message with attachments."""
        emoji = self.SEVERITY_EMOJIS.get(alert.severity, "ℹ️")
        color = self.SEVERITY_COLORS.get(alert.severity, "#2196F3")
        
        fields = [
            {
                "title": "Severity",
                "value": alert.severity.value.upper(),
                "short": True
            },
            {
                "title": "Type",
                "value": alert.alert_type.value.replace("_", " ").title(),
                "short": True
            }
        ]
        
        if alert.sync_id:
            fields.append({
                "title": "Sync ID",
                "value": alert.sync_id,
                "short": True
            })
        
        if alert.model_name:
            fields.append({
                "title": "Model",
                "value": alert.model_name,
                "short": True
            })
        
        if alert.error_code:
            fields.append({
                "title": "Error Code",
                "value": alert.error_code,
                "short": True
            })
        
        return {
            "text": f"{emoji} *{alert.title}*",
            "attachments": [
                {
                    "color": color,
                    "text": alert.message,
                    "fields": fields,
                    "footer": "Fabric-Snowflake Sync",
                    "ts": int(alert.timestamp.timestamp())
                }
            ]
        }


class TeamsAlertChannel(AlertChannel):
    """Microsoft Teams webhook alert channel."""
    
    SEVERITY_COLORS = {
        AlertSeverity.INFO: "0078D4",
        AlertSeverity.WARNING: "FFB900",
        AlertSeverity.ERROR: "E81123",
        AlertSeverity.CRITICAL: "8E0000"
    }
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv("TEAMS_WEBHOOK_URL", "")
    
    def is_configured(self) -> bool:
        return bool(self.webhook_url)
    
    def send(self, alert: Alert) -> bool:
        if not self.is_configured():
            logger.warning("Teams webhook not configured")
            return False
        
        try:
            payload = self._format_message(alert)
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Teams alert sent: {alert.alert_id}")
                return True
            else:
                logger.error(f"Teams send failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Teams alert error: {e}")
            return False
    
    def _format_message(self, alert: Alert) -> Dict[str, Any]:
        """Format alert as Teams Adaptive Card."""
        color = self.SEVERITY_COLORS.get(alert.severity, "0078D4")
        
        facts = [
            {"name": "Severity", "value": alert.severity.value.upper()},
            {"name": "Type", "value": alert.alert_type.value.replace("_", " ").title()},
            {"name": "Timestamp", "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")}
        ]
        
        if alert.sync_id:
            facts.append({"name": "Sync ID", "value": alert.sync_id})
        
        if alert.model_name:
            facts.append({"name": "Model", "value": alert.model_name})
        
        if alert.error_code:
            facts.append({"name": "Error Code", "value": alert.error_code})
        
        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color,
            "summary": alert.title,
            "sections": [
                {
                    "activityTitle": f"🔄 {alert.title}",
                    "activitySubtitle": "Fabric-Snowflake Sync Alert",
                    "facts": facts,
                    "text": alert.message,
                    "markdown": True
                }
            ]
        }


class EmailAlertChannel(AlertChannel):
    """Email alert channel using SendGrid or SMTP."""
    
    def __init__(self, recipients: Optional[List[str]] = None):
        self.recipients = recipients or os.getenv("ALERT_EMAIL_RECIPIENTS", "").split(",")
        self.sendgrid_key = os.getenv("SENDGRID_API_KEY", "")
        self.smtp_host = os.getenv("SMTP_HOST", "")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USER", "")
        self.smtp_password = os.getenv("SMTP_PASSWORD", "")
        self.from_email = os.getenv("ALERT_FROM_EMAIL", "noreply@company.com")
    
    def is_configured(self) -> bool:
        return bool(self.recipients) and (bool(self.sendgrid_key) or bool(self.smtp_host))
    
    def send(self, alert: Alert) -> bool:
        if not self.is_configured():
            logger.warning("Email not configured")
            return False
        
        try:
            if self.sendgrid_key:
                return self._send_sendgrid(alert)
            elif self.smtp_host:
                return self._send_smtp(alert)
            else:
                return False
                
        except Exception as e:
            logger.error(f"Email alert error: {e}")
            return False
    
    def _send_sendgrid(self, alert: Alert) -> bool:
        """Send via SendGrid API."""
        import json
        
        payload = {
            "personalizations": [
                {"to": [{"email": r.strip()} for r in self.recipients if r.strip()]}
            ],
            "from": {"email": self.from_email},
            "subject": f"[{alert.severity.value.upper()}] {alert.title}",
            "content": [
                {
                    "type": "text/html",
                    "value": self._format_html(alert)
                }
            ]
        }
        
        response = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {self.sendgrid_key}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=10
        )
        
        return response.status_code in (200, 202)
    
    def _send_smtp(self, alert: Alert) -> bool:
        """Send via SMTP."""
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[{alert.severity.value.upper()}] {alert.title}"
        msg["From"] = self.from_email
        msg["To"] = ", ".join(self.recipients)
        
        html = self._format_html(alert)
        msg.attach(MIMEText(html, "html"))
        
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            if self.smtp_user:
                server.login(self.smtp_user, self.smtp_password)
            server.sendmail(self.from_email, self.recipients, msg.as_string())
        
        return True
    
    def _format_html(self, alert: Alert) -> str:
        """Format alert as HTML email."""
        severity_colors = {
            AlertSeverity.INFO: "#2196F3",
            AlertSeverity.WARNING: "#FFC107",
            AlertSeverity.ERROR: "#FF5722",
            AlertSeverity.CRITICAL: "#F44336"
        }
        
        color = severity_colors.get(alert.severity, "#2196F3")
        
        details = ""
        if alert.sync_id:
            details += f"<tr><td><strong>Sync ID:</strong></td><td>{alert.sync_id}</td></tr>"
        if alert.model_name:
            details += f"<tr><td><strong>Model:</strong></td><td>{alert.model_name}</td></tr>"
        if alert.error_code:
            details += f"<tr><td><strong>Error Code:</strong></td><td>{alert.error_code}</td></tr>"
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; }}
                .header {{ background: {color}; color: white; padding: 20px; }}
                .header h1 {{ margin: 0; font-size: 20px; }}
                .content {{ padding: 20px; }}
                .severity {{ display: inline-block; padding: 4px 12px; border-radius: 4px; background: {color}; color: white; font-size: 12px; text-transform: uppercase; }}
                .message {{ background: #f9f9f9; padding: 15px; border-radius: 4px; margin: 15px 0; }}
                table {{ width: 100%; border-collapse: collapse; }}
                td {{ padding: 8px 0; border-bottom: 1px solid #eee; }}
                .footer {{ padding: 15px 20px; background: #f5f5f5; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🔄 Fabric-Snowflake Sync Alert</h1>
                </div>
                <div class="content">
                    <h2>{alert.title}</h2>
                    <span class="severity">{alert.severity.value}</span>
                    <p><strong>Type:</strong> {alert.alert_type.value.replace('_', ' ').title()}</p>
                    
                    <div class="message">
                        {alert.message}
                    </div>
                    
                    <table>
                        <tr><td><strong>Timestamp:</strong></td><td>{alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</td></tr>
                        {details}
                    </table>
                </div>
                <div class="footer">
                    This is an automated alert from the Fabric-Snowflake Sync system.
                </div>
            </div>
        </body>
        </html>
        """


class PagerDutyAlertChannel(AlertChannel):
    """PagerDuty alert channel for critical incidents."""
    
    def __init__(self, routing_key: Optional[str] = None):
        self.routing_key = routing_key or os.getenv("PAGERDUTY_ROUTING_KEY", "")
        self.events_url = "https://events.pagerduty.com/v2/enqueue"
    
    def is_configured(self) -> bool:
        return bool(self.routing_key)
    
    def send(self, alert: Alert) -> bool:
        if not self.is_configured():
            logger.warning("PagerDuty not configured")
            return False
        
        # Only send critical alerts to PagerDuty
        if alert.severity != AlertSeverity.CRITICAL:
            logger.debug("Skipping PagerDuty for non-critical alert")
            return True
        
        try:
            payload = {
                "routing_key": self.routing_key,
                "event_action": "trigger",
                "dedup_key": f"{alert.alert_type.value}_{alert.sync_id or alert.alert_id}",
                "payload": {
                    "summary": alert.title,
                    "source": "fabric-snowflake-sync",
                    "severity": "critical",
                    "timestamp": alert.timestamp.isoformat(),
                    "custom_details": {
                        "message": alert.message,
                        "sync_id": alert.sync_id,
                        "model_name": alert.model_name,
                        "error_code": alert.error_code,
                        **alert.metadata
                    }
                }
            }
            
            response = requests.post(
                self.events_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 202:
                logger.info(f"PagerDuty incident created: {alert.alert_id}")
                return True
            else:
                logger.error(f"PagerDuty send failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"PagerDuty alert error: {e}")
            return False


class AlertManager:
    """Central alert manager coordinating all channels."""
    
    def __init__(self):
        self.channels: List[AlertChannel] = []
        self._setup_channels()
        
        # Alert history for deduplication
        self.recent_alerts: Dict[str, datetime] = {}
        self.dedup_window_seconds = 300  # 5 minutes
    
    def _setup_channels(self):
        """Initialize configured channels."""
        # Slack
        slack = SlackAlertChannel()
        if slack.is_configured():
            self.channels.append(slack)
            logger.info("Slack alert channel configured")
        
        # Teams
        teams = TeamsAlertChannel()
        if teams.is_configured():
            self.channels.append(teams)
            logger.info("Teams alert channel configured")
        
        # Email
        email = EmailAlertChannel()
        if email.is_configured():
            self.channels.append(email)
            logger.info("Email alert channel configured")
        
        # PagerDuty
        pagerduty = PagerDutyAlertChannel()
        if pagerduty.is_configured():
            self.channels.append(pagerduty)
            logger.info("PagerDuty alert channel configured")
    
    def send_alert(self, alert: Alert) -> Dict[str, bool]:
        """Send alert to all configured channels."""
        
        # Deduplication check
        dedup_key = f"{alert.alert_type.value}:{alert.severity.value}:{alert.title}"
        if dedup_key in self.recent_alerts:
            last_sent = self.recent_alerts[dedup_key]
            if (alert.timestamp - last_sent).total_seconds() < self.dedup_window_seconds:
                logger.info(f"Deduplicating alert: {dedup_key}")
                return {"deduplicated": True}
        
        self.recent_alerts[dedup_key] = alert.timestamp
        
        # Send to all channels
        results = {}
        for channel in self.channels:
            channel_name = channel.__class__.__name__
            try:
                results[channel_name] = channel.send(alert)
            except Exception as e:
                logger.error(f"Error sending to {channel_name}: {e}")
                results[channel_name] = False
        
        return results
    
    def send_sync_success(self, sync_id: str, models_processed: int, 
                          duration_ms: int) -> Dict[str, bool]:
        """Send sync success notification."""
        alert = Alert(
            alert_id=f"success_{sync_id}",
            alert_type=AlertType.SYNC_SUCCESS,
            severity=AlertSeverity.INFO,
            title="Sync Completed Successfully",
            message=f"Sync {sync_id} completed successfully.\n\n"
                    f"• Models processed: {models_processed}\n"
                    f"• Duration: {duration_ms/1000:.2f} seconds",
            sync_id=sync_id,
            metadata={
                "models_processed": models_processed,
                "duration_ms": duration_ms
            }
        )
        return self.send_alert(alert)
    
    def send_sync_failure(self, sync_id: str, error_message: str,
                          error_code: Optional[str] = None,
                          model_name: Optional[str] = None) -> Dict[str, bool]:
        """Send sync failure alert."""
        alert = Alert(
            alert_id=f"failure_{sync_id}",
            alert_type=AlertType.SYNC_FAILURE,
            severity=AlertSeverity.CRITICAL,
            title="Sync Failed",
            message=f"Sync {sync_id} has failed.\n\n"
                    f"Error: {error_message}",
            sync_id=sync_id,
            error_code=error_code,
            model_name=model_name
        )
        return self.send_alert(alert)
    
    def send_health_alert(self, status: str, issues: List[str]) -> Dict[str, bool]:
        """Send health check alert."""
        severity = AlertSeverity.WARNING if status == "WARNING" else AlertSeverity.CRITICAL
        
        alert = Alert(
            alert_id=f"health_{datetime.now().strftime('%Y%m%d%H%M')}",
            alert_type=AlertType.HEALTH_CHECK,
            severity=severity,
            title=f"Health Check: {status}",
            message="System health check detected issues:\n\n• " + "\n• ".join(issues),
            metadata={"issues": issues}
        )
        return self.send_alert(alert)
    
    def send_data_drift_alert(self, model_name: str, drift_score: float) -> Dict[str, bool]:
        """Send data drift detection alert."""
        severity = AlertSeverity.WARNING if drift_score < 0.25 else AlertSeverity.ERROR
        
        alert = Alert(
            alert_id=f"drift_{model_name}_{datetime.now().strftime('%Y%m%d')}",
            alert_type=AlertType.DATA_DRIFT,
            severity=severity,
            title=f"Data Drift Detected: {model_name}",
            message=f"Significant data drift detected in model {model_name}.\n\n"
                    f"Drift Score: {drift_score:.2%}\n"
                    f"Threshold: 10%\n\n"
                    f"Please review the model for potential issues.",
            model_name=model_name,
            metadata={"drift_score": drift_score}
        )
        return self.send_alert(alert)


# Factory function for creating alert manager
def create_alert_manager() -> AlertManager:
    """Create and return configured alert manager."""
    return AlertManager()


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    manager = create_alert_manager()
    
    # Test alerts
    print("Testing alert channels...")
    
    # Sync success
    results = manager.send_sync_success(
        sync_id="TEST_123",
        models_processed=5,
        duration_ms=5000
    )
    print(f"Success alert results: {results}")
    
    # Sync failure
    results = manager.send_sync_failure(
        sync_id="TEST_456",
        error_message="Connection timeout to Snowflake",
        error_code="SF_CONN_TIMEOUT"
    )
    print(f"Failure alert results: {results}")
