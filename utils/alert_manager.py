"""
Treasury Yield Curve Alert System
Monitors data quality and triggers alerts on anomalies
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple
from dataclasses import dataclass
from enum import Enum
import requests
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "ℹ️ INFO"
    WARNING = "⚠️ WARNING"
    CRITICAL = "🚨 CRITICAL"


class AlertType(Enum):
    """Types of alerts"""
    MISSING_TENORS = "missing_tenors"
    YIELD_JUMP = "yield_jump"
    DUPLICATE_DATA = "duplicate_data"
    NULL_RATES = "null_rates"
    MISSING_DATA = "missing_data"
    SCHEMA_ERROR = "schema_error"
    API_FAILURE = "api_failure"
    S3_FAILURE = "s3_failure"


@dataclass
class Alert:
    """Alert data structure"""
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    timestamp: str
    details: Dict
    
    def to_dict(self) -> Dict:
        return {
            "type": self.alert_type.value,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp,
            "details": self.details,
        }


class AlertManager:
    """Manages alert creation and notification"""
    
    # Expected Treasury rate tenors (in months)
    EXPECTED_TENORS = {1, 3, 6, 12, 24, 60, 120, 360}
    
    # Yield jump threshold in basis points (50 bps)
    YIELD_JUMP_THRESHOLD = 0.50
    
    def __init__(self, slack_webhook_url: str = None, sns_topic_arn: str = None):
        """
        Initialize alert manager
        
        Args:
            slack_webhook_url: Slack webhook URL for notifications
            sns_topic_arn: AWS SNS topic ARN for alerts
        """
        self.slack_webhook_url = slack_webhook_url
        self.sns_topic_arn = sns_topic_arn
        self.sns_client = boto3.client("sns") if sns_topic_arn else None
        self.alerts: List[Alert] = []
    
    def validate_tenors(self, records: List[Dict]) -> List[Alert]:
        """
        Check for missing expected tenors
        
        Args:
            records: List of Treasury rate records
            
        Returns:
            List of alerts for missing tenors
        """
        alerts = []
        
        if not records:
            alert = Alert(
                alert_type=AlertType.MISSING_DATA,
                severity=AlertSeverity.CRITICAL,
                title="No Data Records",
                message="No Treasury rate records were fetched",
                timestamp=datetime.utcnow().isoformat(),
                details={"record_count": 0}
            )
            alerts.append(alert)
            return alerts
        
        current_tenors = set(
            int(rec.get("maturity_length_months", 0)) 
            for rec in records 
            if "maturity_length_months" in rec
        )
        
        # Find missing tenors
        missing_tenors = self.EXPECTED_TENORS - current_tenors
        
        if missing_tenors:
            alert = Alert(
                alert_type=AlertType.MISSING_TENORS,
                severity=AlertSeverity.WARNING,
                title="Missing Treasury Tenors",
                message=f"Expected tenors not found: {sorted(missing_tenors)}",
                timestamp=datetime.utcnow().isoformat(),
                details={
                    "expected_tenors": sorted(self.EXPECTED_TENORS),
                    "found_tenors": sorted(current_tenors),
                    "missing_tenors": sorted(missing_tenors),
                }
            )
            alerts.append(alert)
        
        return alerts
    
    def check_null_rates(self, records: List[Dict]) -> List[Alert]:
        """
        Check for null or missing rate values
        
        Args:
            records: List of Treasury rate records
            
        Returns:
            List of alerts for null rates
        """
        alerts = []
        null_records = []
        
        for idx, rec in enumerate(records):
            rate = rec.get("rate")
            if rate is None or rate == "" or (isinstance(rate, str) and rate.lower() == "null"):
                null_records.append({
                    "index": idx,
                    "tenor": rec.get("maturity_length_months", "unknown"),
                    "date": rec.get("record_date", "unknown")
                })
        
        if null_records:
            alert = Alert(
                alert_type=AlertType.NULL_RATES,
                severity=AlertSeverity.WARNING,
                title="Null Rate Values Found",
                message=f"{len(null_records)} records have null/missing rates",
                timestamp=datetime.utcnow().isoformat(),
                details={
                    "null_count": len(null_records),
                    "null_records": null_records[:5],  # First 5
                    "total_records": len(records)
                }
            )
            alerts.append(alert)
        
        return alerts
    
    def check_duplicates(self, records: List[Dict]) -> List[Alert]:
        """
        Check for duplicate records
        
        Args:
            records: List of Treasury rate records
            
        Returns:
            List of alerts for duplicates
        """
        alerts = []
        seen = {}
        duplicates = []
        
        for rec in records:
            key = (rec.get("record_date"), rec.get("maturity_length_months"))
            if key in seen:
                duplicates.append({
                    "date": rec.get("record_date"),
                    "tenor": rec.get("maturity_length_months"),
                    "count": seen[key] + 1
                })
            seen[key] = seen.get(key, 0) + 1
        
        if duplicates:
            alert = Alert(
                alert_type=AlertType.DUPLICATE_DATA,
                severity=AlertSeverity.WARNING,
                title="Duplicate Records Found",
                message=f"{len(duplicates)} duplicate date-tenor combinations detected",
                timestamp=datetime.utcnow().isoformat(),
                details={
                    "duplicate_count": len(duplicates),
                    "duplicates": duplicates[:5],  # First 5
                }
            )
            alerts.append(alert)
        
        return alerts
    
    def check_yield_jumps(self, prev_records: List[Dict], current_records: List[Dict]) -> List[Alert]:
        """
        Check for abnormal yield movements (jumps)
        
        Args:
            prev_records: Previous batch of Treasury rates
            current_records: Current batch of Treasury rates
            
        Returns:
            List of alerts for abnormal moves
        """
        alerts = []
        
        if not prev_records:
            logger.info("No previous records to compare; skipping yield jump check")
            return alerts
        
        # Create lookup dictionaries by tenor
        prev_rates = {
            int(rec.get("maturity_length_months", 0)): float(rec.get("rate", 0))
            for rec in prev_records
        }
        current_rates = {
            int(rec.get("maturity_length_months", 0)): float(rec.get("rate", 0))
            for rec in current_records
        }
        
        # Check for jumps
        anomalies = []
        for tenor, current_rate in current_rates.items():
            if tenor in prev_rates:
                prev_rate = prev_rates[tenor]
                jump = abs(current_rate - prev_rate)
                
                if jump > self.YIELD_JUMP_THRESHOLD:
                    anomalies.append({
                        "tenor": tenor,
                        "previous_rate": prev_rate,
                        "current_rate": current_rate,
                        "jump_bps": round(jump * 100, 2),  # Convert to basis points
                    })
        
        if anomalies:
            alert = Alert(
                alert_type=AlertType.YIELD_JUMP,
                severity=AlertSeverity.WARNING,
                title="Abnormal Yield Movements Detected",
                message=f"{len(anomalies)} tenors moved more than {self.YIELD_JUMP_THRESHOLD*100:.0f} bps",
                timestamp=datetime.utcnow().isoformat(),
                details={
                    "anomaly_count": len(anomalies),
                    "anomalies": anomalies,
                    "threshold_bps": self.YIELD_JUMP_THRESHOLD * 100,
                }
            )
            alerts.append(alert)
        
        return alerts
    
    def validate_batch(self, records: List[Dict], prev_records: List[Dict] = None) -> List[Alert]:
        """
        Run all validation checks on a batch of records
        
        Args:
            records: Current batch of Treasury rates
            prev_records: Previous batch for comparison
            
        Returns:
            List of all alerts
        """
        all_alerts = []
        
        # Run all checks
        all_alerts.extend(self.validate_tenors(records))
        all_alerts.extend(self.check_null_rates(records))
        all_alerts.extend(self.check_duplicates(records))
        
        if prev_records:
            all_alerts.extend(self.check_yield_jumps(prev_records, records))
        
        self.alerts.extend(all_alerts)
        return all_alerts
    
    def send_slack_notification(self, alert: Alert) -> bool:
        """
        Send alert to Slack
        
        Args:
            alert: Alert to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.slack_webhook_url:
            logger.warning("Slack webhook URL not configured")
            return False
        
        try:
            color_map = {
                AlertSeverity.INFO: "#36a64f",      # Green
                AlertSeverity.WARNING: "#ffa500",   # Orange
                AlertSeverity.CRITICAL: "#ff0000",  # Red
            }
            
            slack_message = {
                "attachments": [
                    {
                        "color": color_map.get(alert.severity, "#cccccc"),
                        "title": f"{alert.severity.value} {alert.title}",
                        "text": alert.message,
                        "fields": [
                            {
                                "title": "Alert Type",
                                "value": alert.alert_type.value,
                                "short": True
                            },
                            {
                                "title": "Timestamp",
                                "value": alert.timestamp,
                                "short": True
                            },
                        ],
                        "footer": "Treasury Yield Curve Alert System",
                        "ts": int(datetime.utcnow().timestamp())
                    }
                ]
            }
            
            response = requests.post(
                self.slack_webhook_url,
                json=slack_message,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info(f"Slack notification sent: {alert.title}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False
    
    def send_sns_notification(self, alert: Alert) -> bool:
        """
        Send alert to AWS SNS
        
        Args:
            alert: Alert to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.sns_client or not self.sns_topic_arn:
            logger.warning("SNS not configured")
            return False
        
        try:
            message = f"""
{alert.severity.value} {alert.title}

Message: {alert.message}
Type: {alert.alert_type.value}
Timestamp: {alert.timestamp}

Details:
{json.dumps(alert.details, indent=2)}
            """.strip()
            
            response = self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Subject=f"[{alert.severity.value}] {alert.title}",
                Message=message,
            )
            
            logger.info(f"SNS notification sent: {response['MessageId']}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to send SNS notification: {e}")
            return False
    
    def notify(self, alert: Alert) -> bool:
        """
        Send alert via all configured channels
        
        Args:
            alert: Alert to send
            
        Returns:
            True if sent successfully
        """
        sent = False
        
        # Try Slack first
        if self.slack_webhook_url:
            sent = self.send_slack_notification(alert) or sent
        
        # Try SNS
        if self.sns_topic_arn:
            sent = self.send_sns_notification(alert) or sent
        
        # Always log
        logger.info(f"Alert: {alert.to_dict()}")
        
        return sent
    
    def notify_all(self, alerts: List[Alert]) -> None:
        """
        Send all alerts
        
        Args:
            alerts: List of alerts to send
        """
        for alert in alerts:
            self.notify(alert)