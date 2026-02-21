"""
Anomaly Detection Alerting Module

Provides alerting mechanisms for data pipeline anomalies including:
- Email notifications via SMTP
- Slack webhook notifications
- Console/logging alerts

Usage:
    from scripts.alerting import AlertManager

    alert_manager = AlertManager()
    alert_manager.send_alert(
        alert_type="ANOMALY",
        severity="HIGH",
        message="Data drift detected: entity_type distribution shifted by 15%",
        details={"metric": "entity_type_distribution", "drift_pct": 15.2}
    )
"""

import json
import logging
import os
import smtplib
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AlertType(Enum):
    """Types of alerts."""

    ANOMALY = "ANOMALY"
    DATA_DRIFT = "DATA_DRIFT"
    SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
    PIPELINE_FAILURE = "PIPELINE_FAILURE"
    BIAS_DETECTED = "BIAS_DETECTED"
    THRESHOLD_BREACH = "THRESHOLD_BREACH"


@dataclass
class Alert:
    """Represents a single alert."""

    alert_type: str
    severity: str
    message: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    details: Dict[str, Any] = field(default_factory=dict)
    pipeline_run_id: Optional[str] = None
    dataset: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "alert_type": self.alert_type,
            "severity": self.severity,
            "message": self.message,
            "timestamp": self.timestamp,
            "details": self.details,
            "pipeline_run_id": self.pipeline_run_id,
            "dataset": self.dataset,
        }

    def to_json(self) -> str:
        """Convert alert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class EmailNotifier:
    """Send alerts via email using SMTP."""

    def __init__(
        self,
        smtp_host: str = None,
        smtp_port: int = 587,
        username: str = None,
        password: str = None,
        from_email: str = None,
        to_emails: List[str] = None,
    ):
        """Initialize email notifier with SMTP settings."""
        self.smtp_host = smtp_host or os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = smtp_port or int(os.getenv("SMTP_PORT", "587"))
        self.username = username or os.getenv("SMTP_USERNAME")
        self.password = password or os.getenv("SMTP_PASSWORD")
        self.from_email = from_email or os.getenv(
            "ALERT_FROM_EMAIL", "alerts@entity-resolution.io"
        )
        self.to_emails = to_emails or os.getenv("ALERT_TO_EMAILS", "").split(",")

    def send(self, alert: Alert) -> bool:
        """Send alert via email."""
        if not self.username or not self.password:
            logger.warning(
                "Email credentials not configured. Skipping email notification."
            )
            return False

        if not self.to_emails or self.to_emails == [""]:
            logger.warning(
                "No recipient emails configured. Skipping email notification."
            )
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = (
                f"[{alert.severity}] Entity Resolution Alert: {alert.alert_type}"
            )
            msg["From"] = self.from_email
            msg["To"] = ", ".join(self.to_emails)

            # Plain text body
            text_body = f"""
Entity Resolution Pipeline Alert
================================

Type: {alert.alert_type}
Severity: {alert.severity}
Timestamp: {alert.timestamp}
Dataset: {alert.dataset or 'N/A'}
Pipeline Run: {alert.pipeline_run_id or 'N/A'}

Message:
{alert.message}

Details:
{json.dumps(alert.details, indent=2)}

---
This is an automated alert from the Entity Resolution Data Pipeline.
            """

            # HTML body
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h2 style="color: {'#dc3545' if alert.severity in ['HIGH', 'CRITICAL'] else '#ffc107'};">
                    Entity Resolution Pipeline Alert
                </h2>
                <table style="border-collapse: collapse; width: 100%;">
                    <tr><td><strong>Type:</strong></td><td>{alert.alert_type}</td></tr>
                    <tr><td><strong>Severity:</strong></td><td>
                        <span style="color: {'#dc3545' if alert.severity in ['HIGH', 'CRITICAL'] else '#ffc107'};">
                            {alert.severity}
                        </span>
                    </td></tr>
                    <tr><td><strong>Timestamp:</strong></td><td>{alert.timestamp}</td></tr>
                    <tr><td><strong>Dataset:</strong></td><td>{alert.dataset or 'N/A'}</td></tr>
                    <tr><td><strong>Pipeline Run:</strong></td><td>{alert.pipeline_run_id or 'N/A'}</td></tr>
                </table>
                <h3>Message</h3>
                <p style="background: #f8f9fa; padding: 10px; border-radius: 5px;">
                    {alert.message}
                </p>
                <h3>Details</h3>
                <pre style="background: #f8f9fa; padding: 10px; border-radius: 5px;">
{json.dumps(alert.details, indent=2)}
                </pre>
                <hr>
                <p style="color: #6c757d; font-size: 12px;">
                    This is an automated alert from the Entity Resolution Data Pipeline.
                </p>
            </body>
            </html>
            """

            msg.attach(MIMEText(text_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.sendmail(self.from_email, self.to_emails, msg.as_string())

            logger.info(f"Email alert sent to {self.to_emails}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False


class SlackNotifier:
    """Send alerts via Slack webhook."""

    def __init__(self, webhook_url: str = None):
        """Initialize Slack notifier with webhook URL."""
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")

    def send(self, alert: Alert) -> bool:
        """Send alert via Slack webhook."""
        if not self.webhook_url:
            logger.warning(
                "Slack webhook URL not configured. Skipping Slack notification."
            )
            return False

        try:
            import urllib.request

            # Color based on severity
            color_map = {
                "LOW": "#28a745",  # Green
                "MEDIUM": "#ffc107",  # Yellow
                "HIGH": "#fd7e14",  # Orange
                "CRITICAL": "#dc3545",  # Red
            }
            color = color_map.get(alert.severity, "#6c757d")

            # Build Slack message
            slack_message = {
                "attachments": [
                    {
                        "color": color,
                        "title": f":warning: {alert.alert_type}: {alert.message[:100]}",
                        "fields": [
                            {
                                "title": "Severity",
                                "value": alert.severity,
                                "short": True,
                            },
                            {"title": "Type", "value": alert.alert_type, "short": True},
                            {
                                "title": "Timestamp",
                                "value": alert.timestamp,
                                "short": True,
                            },
                            {
                                "title": "Dataset",
                                "value": alert.dataset or "N/A",
                                "short": True,
                            },
                        ],
                        "text": f"```{json.dumps(alert.details, indent=2)}```",
                        "footer": "Entity Resolution Pipeline",
                        "ts": int(datetime.utcnow().timestamp()),
                    }
                ]
            }

            data = json.dumps(slack_message).encode("utf-8")
            req = urllib.request.Request(
                self.webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
            )

            with urllib.request.urlopen(req) as response:
                if response.status == 200:
                    logger.info("Slack alert sent successfully")
                    return True

        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False

        return False


class ConsoleNotifier:
    """Log alerts to console/logging system."""

    def send(self, alert: Alert) -> bool:
        """Log alert to console."""
        log_message = (
            f"\n{'='*60}\n"
            f"ALERT: {alert.alert_type} [{alert.severity}]\n"
            f"{'='*60}\n"
            f"Time: {alert.timestamp}\n"
            f"Dataset: {alert.dataset or 'N/A'}\n"
            f"Message: {alert.message}\n"
            f"Details: {json.dumps(alert.details, indent=2)}\n"
            f"{'='*60}\n"
        )

        if alert.severity in ["HIGH", "CRITICAL"]:
            logger.error(log_message)
        elif alert.severity == "MEDIUM":
            logger.warning(log_message)
        else:
            logger.info(log_message)

        return True


class AlertManager:
    """
    Central alert management system.

    Coordinates multiple notification channels and provides
    anomaly detection alerting for the data pipeline.
    """

    def __init__(
        self,
        enable_email: bool = True,
        enable_slack: bool = True,
        enable_console: bool = True,
        alert_history_file: str = None,
    ):
        """Initialize alert manager with notification channels."""
        self.notifiers = []

        if enable_console:
            self.notifiers.append(ConsoleNotifier())

        if enable_email:
            self.notifiers.append(EmailNotifier())

        if enable_slack:
            self.notifiers.append(SlackNotifier())

        self.alert_history_file = alert_history_file or os.getenv(
            "ALERT_HISTORY_FILE", "/tmp/entity_resolution_alerts.json"
        )
        self.alert_history: List[Alert] = []

    def send_alert(
        self,
        alert_type: str,
        severity: str,
        message: str,
        details: Dict[str, Any] = None,
        pipeline_run_id: str = None,
        dataset: str = None,
    ) -> Alert:
        """
        Send an alert through all configured channels.

        Args:
            alert_type: Type of alert (ANOMALY, DATA_DRIFT, etc.)
            severity: Alert severity (LOW, MEDIUM, HIGH, CRITICAL)
            message: Human-readable alert message
            details: Additional context and metrics
            pipeline_run_id: Optional pipeline run identifier
            dataset: Optional dataset name

        Returns:
            The Alert object that was sent
        """
        alert = Alert(
            alert_type=alert_type,
            severity=severity,
            message=message,
            details=details or {},
            pipeline_run_id=pipeline_run_id,
            dataset=dataset,
        )

        # Send through all notifiers
        for notifier in self.notifiers:
            try:
                notifier.send(alert)
            except Exception as e:
                logger.error(f"Notifier {type(notifier).__name__} failed: {e}")

        # Store in history
        self.alert_history.append(alert)
        self._save_alert_history()

        return alert

    def _save_alert_history(self):
        """Save alert history to file."""
        try:
            history_data = [
                a.to_dict() for a in self.alert_history[-100:]
            ]  # Keep last 100
            with open(self.alert_history_file, "w") as f:
                json.dump(history_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save alert history: {e}")

    # Convenience methods for common anomaly patterns

    def alert_data_drift(
        self,
        metric_name: str,
        drift_percentage: float,
        expected_value: Any,
        actual_value: Any,
        dataset: str = None,
    ):
        """Alert on data drift detection."""
        severity = (
            "HIGH"
            if drift_percentage > 20
            else "MEDIUM" if drift_percentage > 10 else "LOW"
        )

        return self.send_alert(
            alert_type="DATA_DRIFT",
            severity=severity,
            message=f"Data drift detected in {metric_name}: {drift_percentage:.1f}% deviation",
            details={
                "metric": metric_name,
                "drift_percentage": drift_percentage,
                "expected": expected_value,
                "actual": actual_value,
            },
            dataset=dataset,
        )

    def alert_schema_violation(
        self,
        expectation: str,
        column: str,
        failure_details: Dict[str, Any],
        dataset: str = None,
    ):
        """Alert on schema validation failure."""
        return self.send_alert(
            alert_type="SCHEMA_VIOLATION",
            severity="HIGH",
            message=f"Schema validation failed: {expectation} on column '{column}'",
            details={
                "expectation": expectation,
                "column": column,
                "failure_details": failure_details,
            },
            dataset=dataset,
        )

    def alert_bias_detected(
        self,
        bias_type: str,
        affected_groups: List[str],
        disparity_ratio: float,
        dataset: str = None,
    ):
        """Alert on bias detection."""
        severity = "HIGH" if disparity_ratio > 0.5 else "MEDIUM"

        return self.send_alert(
            alert_type="BIAS_DETECTED",
            severity=severity,
            message=f"Bias detected: {bias_type} affecting {len(affected_groups)} groups",
            details={
                "bias_type": bias_type,
                "affected_groups": affected_groups,
                "disparity_ratio": disparity_ratio,
            },
            dataset=dataset,
        )

    def alert_pipeline_failure(
        self,
        task_name: str,
        error_message: str,
        stack_trace: str = None,
        pipeline_run_id: str = None,
    ):
        """Alert on pipeline task failure."""
        return self.send_alert(
            alert_type="PIPELINE_FAILURE",
            severity="CRITICAL",
            message=f"Pipeline task '{task_name}' failed: {error_message[:200]}",
            details={
                "task": task_name,
                "error": error_message,
                "stack_trace": stack_trace,
            },
            pipeline_run_id=pipeline_run_id,
        )

    def alert_threshold_breach(
        self,
        metric_name: str,
        threshold: float,
        actual_value: float,
        direction: str = "above",
        dataset: str = None,
    ):
        """Alert when a metric breaches a threshold."""
        severity = (
            "HIGH" if abs(actual_value - threshold) / threshold > 0.5 else "MEDIUM"
        )

        return self.send_alert(
            alert_type="THRESHOLD_BREACH",
            severity=severity,
            message=f"Threshold breach: {metric_name} is {direction} threshold ({actual_value:.2f} vs {threshold:.2f})",
            details={
                "metric": metric_name,
                "threshold": threshold,
                "actual_value": actual_value,
                "direction": direction,
                "breach_percentage": abs(actual_value - threshold) / threshold * 100,
            },
            dataset=dataset,
        )


# Singleton instance for easy access
_alert_manager = None


def get_alert_manager() -> AlertManager:
    """Get or create the singleton AlertManager instance."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


# Example usage and testing
if __name__ == "__main__":
    # Demo the alerting system
    manager = AlertManager(enable_email=False, enable_slack=False, enable_console=True)

    # Test various alert types
    manager.alert_data_drift(
        metric_name="entity_type_distribution",
        drift_percentage=15.2,
        expected_value={"PERSON": 0.5, "PRODUCT": 0.3, "PUBLICATION": 0.2},
        actual_value={"PERSON": 0.35, "PRODUCT": 0.45, "PUBLICATION": 0.2},
        dataset="combined_multi_domain",
    )

    manager.alert_schema_violation(
        expectation="expect_column_values_to_not_be_null",
        column="account_id",
        failure_details={"null_count": 150, "null_percentage": 3.2},
        dataset="pseudopeople",
    )

    manager.alert_bias_detected(
        bias_type="geographic_bias",
        affected_groups=["NC", "CA", "TX"],
        disparity_ratio=0.45,
        dataset="nc_voters",
    )

    print(f"\nTotal alerts sent: {len(manager.alert_history)}")
    print(f"Alert history saved to: {manager.alert_history_file}")
