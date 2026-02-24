"""
Tests for the Alerting Module.
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from scripts.alerting import (
    Alert,
    AlertManager,
    AlertSeverity,
    AlertType,
    ConsoleNotifier,
    EmailNotifier,
    SlackNotifier,
    get_alert_manager,
)


class TestAlert:
    """Tests for Alert dataclass."""

    def test_alert_creation(self):
        """Test basic alert creation."""
        alert = Alert(
            alert_type="ANOMALY", severity="HIGH", message="Test alert message"
        )
        assert alert.alert_type == "ANOMALY"
        assert alert.severity == "HIGH"
        assert alert.message == "Test alert message"
        assert alert.timestamp is not None
        assert alert.details == {}

    def test_alert_with_details(self):
        """Test alert with additional details."""
        alert = Alert(
            alert_type="DATA_DRIFT",
            severity="MEDIUM",
            message="Drift detected",
            details={"metric": "distribution", "drift_pct": 15.2},
            dataset="test_dataset",
            pipeline_run_id="run_123",
        )
        assert alert.details["drift_pct"] == 15.2
        assert alert.dataset == "test_dataset"
        assert alert.pipeline_run_id == "run_123"

    def test_alert_to_dict(self):
        """Test converting alert to dictionary."""
        alert = Alert(
            alert_type="SCHEMA_VIOLATION", severity="CRITICAL", message="Schema failed"
        )
        alert_dict = alert.to_dict()
        assert isinstance(alert_dict, dict)
        assert alert_dict["alert_type"] == "SCHEMA_VIOLATION"
        assert alert_dict["severity"] == "CRITICAL"

    def test_alert_to_json(self):
        """Test converting alert to JSON."""
        alert = Alert(alert_type="BIAS_DETECTED", severity="LOW", message="Minor bias")
        alert_json = alert.to_json()
        assert isinstance(alert_json, str)
        parsed = json.loads(alert_json)
        assert parsed["alert_type"] == "BIAS_DETECTED"


class TestAlertEnums:
    """Tests for alert enums."""

    def test_alert_severity_values(self):
        """Test AlertSeverity enum values."""
        assert AlertSeverity.LOW.value == "LOW"
        assert AlertSeverity.MEDIUM.value == "MEDIUM"
        assert AlertSeverity.HIGH.value == "HIGH"
        assert AlertSeverity.CRITICAL.value == "CRITICAL"

    def test_alert_type_values(self):
        """Test AlertType enum values."""
        assert AlertType.ANOMALY.value == "ANOMALY"
        assert AlertType.DATA_DRIFT.value == "DATA_DRIFT"
        assert AlertType.SCHEMA_VIOLATION.value == "SCHEMA_VIOLATION"
        assert AlertType.PIPELINE_FAILURE.value == "PIPELINE_FAILURE"
        assert AlertType.BIAS_DETECTED.value == "BIAS_DETECTED"
        assert AlertType.THRESHOLD_BREACH.value == "THRESHOLD_BREACH"


class TestConsoleNotifier:
    """Tests for ConsoleNotifier."""

    def test_console_notifier_sends_alert(self):
        """Test console notifier logs alert."""
        notifier = ConsoleNotifier()
        alert = Alert(
            alert_type="TEST", severity="LOW", message="Test console notification"
        )
        result = notifier.send(alert)
        assert result is True

    def test_console_notifier_handles_all_severities(self):
        """Test console notifier handles all severity levels."""
        notifier = ConsoleNotifier()
        for severity in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]:
            alert = Alert(
                alert_type="TEST",
                severity=severity,
                message=f"Testing {severity} severity",
            )
            result = notifier.send(alert)
            assert result is True


class TestEmailNotifier:
    """Tests for EmailNotifier."""

    def test_email_notifier_skips_without_credentials(self):
        """Test email notifier skips when no credentials."""
        notifier = EmailNotifier(username=None, password=None)
        alert = Alert(alert_type="TEST", severity="HIGH", message="Test email")
        result = notifier.send(alert)
        assert result is False

    def test_email_notifier_skips_without_recipients(self):
        """Test email notifier skips when no recipients."""
        notifier = EmailNotifier(
            username="test@test.com", password="password", to_emails=[]
        )
        alert = Alert(alert_type="TEST", severity="HIGH", message="Test email")
        result = notifier.send(alert)
        assert result is False


class TestSlackNotifier:
    """Tests for SlackNotifier."""

    def test_slack_notifier_skips_without_webhook(self):
        """Test Slack notifier skips when no webhook URL."""
        notifier = SlackNotifier(webhook_url=None)
        alert = Alert(alert_type="TEST", severity="HIGH", message="Test Slack")
        result = notifier.send(alert)
        assert result is False


class TestAlertManager:
    """Tests for AlertManager."""

    def test_alert_manager_creation(self):
        """Test AlertManager initialization."""
        manager = AlertManager(
            enable_email=False, enable_slack=False, enable_console=True
        )
        assert len(manager.notifiers) >= 1
        assert isinstance(manager.notifiers[0], ConsoleNotifier)

    def test_send_alert(self):
        """Test sending basic alert."""
        manager = AlertManager(
            enable_email=False, enable_slack=False, enable_console=True
        )
        alert = manager.send_alert(
            alert_type="TEST", severity="MEDIUM", message="Test alert from manager"
        )
        assert alert is not None
        assert alert.alert_type == "TEST"
        assert len(manager.alert_history) == 1

    def test_alert_data_drift(self):
        """Test data drift alert convenience method."""
        manager = AlertManager(
            enable_email=False, enable_slack=False, enable_console=True
        )
        alert = manager.alert_data_drift(
            metric_name="entity_distribution",
            drift_percentage=25.0,
            expected_value=0.5,
            actual_value=0.35,
            dataset="test",
        )
        assert alert.alert_type == "DATA_DRIFT"
        assert alert.severity == "HIGH"  # >20% drift = HIGH
        assert alert.details["drift_percentage"] == 25.0

    def test_alert_data_drift_medium_severity(self):
        """Test data drift with medium severity."""
        manager = AlertManager(
            enable_email=False, enable_slack=False, enable_console=True
        )
        alert = manager.alert_data_drift(
            metric_name="distribution",
            drift_percentage=15.0,
            expected_value=0.5,
            actual_value=0.4,
        )
        assert alert.severity == "MEDIUM"  # 10-20% drift = MEDIUM

    def test_alert_schema_violation(self):
        """Test schema violation alert."""
        manager = AlertManager(
            enable_email=False, enable_slack=False, enable_console=True
        )
        alert = manager.alert_schema_violation(
            expectation="expect_column_to_exist",
            column="account_id",
            failure_details={"message": "Column missing"},
        )
        assert alert.alert_type == "SCHEMA_VIOLATION"
        assert alert.severity == "HIGH"

    def test_alert_bias_detected(self):
        """Test bias detection alert."""
        manager = AlertManager(
            enable_email=False, enable_slack=False, enable_console=True
        )
        alert = manager.alert_bias_detected(
            bias_type="geographic_bias",
            affected_groups=["NC", "CA"],
            disparity_ratio=0.6,
        )
        assert alert.alert_type == "BIAS_DETECTED"
        assert alert.severity == "HIGH"  # disparity > 0.5 = HIGH

    def test_alert_pipeline_failure(self):
        """Test pipeline failure alert."""
        manager = AlertManager(
            enable_email=False, enable_slack=False, enable_console=True
        )
        alert = manager.alert_pipeline_failure(
            task_name="data_transformation",
            error_message="Out of memory",
            pipeline_run_id="run_456",
        )
        assert alert.alert_type == "PIPELINE_FAILURE"
        assert alert.severity == "CRITICAL"

    def test_alert_threshold_breach(self):
        """Test threshold breach alert."""
        manager = AlertManager(
            enable_email=False, enable_slack=False, enable_console=True
        )
        alert = manager.alert_threshold_breach(
            metric_name="null_percentage",
            threshold=5.0,
            actual_value=12.0,
            direction="above",
        )
        assert alert.alert_type == "THRESHOLD_BREACH"
        assert alert.details["threshold"] == 5.0
        assert alert.details["actual_value"] == 12.0


class TestGetAlertManager:
    """Tests for singleton alert manager."""

    def test_get_alert_manager_returns_singleton(self):
        """Test that get_alert_manager returns same instance."""
        # Reset singleton for test
        import scripts.alerting as alerting_module

        alerting_module._alert_manager = None

        manager1 = get_alert_manager()
        manager2 = get_alert_manager()
        assert manager1 is manager2
