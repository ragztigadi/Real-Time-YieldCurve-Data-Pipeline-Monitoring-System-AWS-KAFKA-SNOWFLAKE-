"""
ML-Powered Anomaly Detection for Treasury Yield Curve Data
Two-Tier System: Fast Statistical Checks + Deep ML Analysis
"""
import logging
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
from collections import deque

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MLAlertSeverity(Enum):
    """ML alert severity levels"""
    INFO = "ℹ️ INFO"
    WARNING = "⚠️ WARNING"
    CRITICAL = "🚨 CRITICAL"


class MLAlertType(Enum):
    """Types of ML anomaly alerts"""
    ZSCORE_ANOMALY = "zscore_anomaly"
    VOLATILITY_SPIKE = "volatility_spike"
    RATE_CHANGE_ANOMALY = "rate_change_anomaly"
    MULTIVARIATE_ANOMALY = "multivariate_anomaly"
    TREND_BREAK = "trend_break"
    EXTREME_VALUE = "extreme_value"


@dataclass
class MLAlert:
    """ML anomaly alert structure"""
    alert_type: MLAlertType
    severity: MLAlertSeverity
    title: str
    message: str
    timestamp: str
    record_date: str
    tenor: str
    details: Dict
    
    def to_dict(self) -> Dict:
        return {
            "type": self.alert_type.value,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp,
            "record_date": self.record_date,
            "tenor": self.tenor,
            "details": self.details,
        }


class StatisticalAnomalyDetector:
    """
    Tier 1: Fast statistical anomaly detection
    Runs in real-time on streaming data
    """
    
    # Tenors to monitor
    TENORS = [
        "one_year_or_less",
        "between_1_and_5_years",
        "between_5_and_10_years",
        "between_10_and_20_years",
        "twenty_years_or_greater"
    ]
    
    # Thresholds for anomaly detection
    ZSCORE_THRESHOLD = 3.0  # 3 standard deviations
    VOLATILITY_THRESHOLD = 2.5  # 2.5x normal volatility
    RATE_CHANGE_THRESHOLD = 0.5  # 50 basis points change
    EXTREME_VALUE_MIN = -2.0
    EXTREME_VALUE_MAX = 25.0
    
    def __init__(self, window_size: int = 100):
        """
        Initialize detector with rolling window
        
        Args:
            window_size: Number of historical records to keep for statistics
        """
        self.window_size = window_size
        
        # Rolling windows for each tenor (stores historical rates)
        self.rolling_windows = {
            tenor: deque(maxlen=window_size) for tenor in self.TENORS
        }
        
        # Previous batch for rate-of-change calculation
        self.previous_batch: Optional[List[Dict]] = None
        
        logger.info(f" Statistical anomaly detector initialized (window_size={window_size})")
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _calculate_statistics(self, tenor: str) -> Tuple[float, float]:
        """
        Calculate mean and std dev for a tenor
        
        Returns:
            (mean, std_dev) tuple
        """
        window = self.rolling_windows[tenor]
        if len(window) < 10:  # Need minimum data
            return None, None
        
        rates = [r for r in window if r is not None]
        if not rates:
            return None, None
        
        mean = np.mean(rates)
        std = np.std(rates)
        
        return mean, std
    
    def _calculate_volatility(self, tenor: str) -> Optional[float]:
        """
        Calculate rolling volatility (standard deviation of returns)
        """
        window = self.rolling_windows[tenor]
        if len(window) < 20:
            return None
        
        rates = [r for r in window if r is not None]
        if len(rates) < 20:
            return None
        
        # Calculate returns (rate changes)
        returns = [rates[i] - rates[i-1] for i in range(1, len(rates))]
        
        volatility = np.std(returns)
        return volatility

    # ANOMALY DETECTION RULE 1: Z-Score Detection
    
    def detect_zscore_anomalies(self, record: Dict) -> List[MLAlert]:
        """
        Detect anomalies using Z-score (standard deviations from mean)
        
        Z-score > 3.0 indicates statistically significant outlier
        """
        alerts = []
        record_date = record.get("record_date", "unknown")
        
        for tenor in self.TENORS:
            rate = self._safe_float(record.get(tenor))
            if rate is None:
                continue
            
            # Calculate statistics from historical window
            mean, std = self._calculate_statistics(tenor)
            if mean is None or std is None or std == 0:
                # Not enough data yet
                self.rolling_windows[tenor].append(rate)
                continue
            
            # Calculate Z-score
            zscore = abs((rate - mean) / std)
            
            if zscore > self.ZSCORE_THRESHOLD:
                severity = (MLAlertSeverity.CRITICAL if zscore > 4.0 
                           else MLAlertSeverity.WARNING)
                
                alert = MLAlert(
                    alert_type=MLAlertType.ZSCORE_ANOMALY,
                    severity=severity,
                    title=f"Z-Score Anomaly Detected: {tenor}",
                    message=f"Rate {rate}% is {zscore:.2f} std devs from mean on {record_date}",
                    timestamp=datetime.utcnow().isoformat(),
                    record_date=record_date,
                    tenor=tenor,
                    details={
                        "rate": rate,
                        "zscore": round(zscore, 3),
                        "mean": round(mean, 3),
                        "std_dev": round(std, 3),
                        "threshold": self.ZSCORE_THRESHOLD,
                        "interpretation": f"Rate is {zscore:.1f}σ from historical average"
                    }
                )
                alerts.append(alert)
            
            # Add to rolling window
            self.rolling_windows[tenor].append(rate)
        
        return alerts
    # ANOMALY DETECTION RULE 2: Volatility Spike Detection
    
    def detect_volatility_spikes(self, batch: List[Dict]) -> List[MLAlert]:
        """
        Detect sudden increases in volatility (market stress indicator)
        
        Volatility spike indicates market uncertainty or regime change
        """
        alerts = []
        
        if not batch:
            return alerts
        
        for tenor in self.TENORS:
            # Calculate current batch volatility
            batch_rates = [self._safe_float(r.get(tenor)) for r in batch]
            batch_rates = [r for r in batch_rates if r is not None]
            
            if len(batch_rates) < 5:
                continue
            
            current_volatility = np.std(batch_rates)
            
            # Calculate historical volatility
            historical_volatility = self._calculate_volatility(tenor)
            if historical_volatility is None or historical_volatility == 0:
                continue
            
            # Check for volatility spike
            volatility_ratio = current_volatility / historical_volatility
            
            if volatility_ratio > self.VOLATILITY_THRESHOLD:
                record_date = batch[-1].get("record_date", "unknown")
                
                alert = MLAlert(
                    alert_type=MLAlertType.VOLATILITY_SPIKE,
                    severity=MLAlertSeverity.WARNING,
                    title=f"Volatility Spike: {tenor}",
                    message=f"Volatility increased {volatility_ratio:.1f}x on {record_date}",
                    timestamp=datetime.utcnow().isoformat(),
                    record_date=record_date,
                    tenor=tenor,
                    details={
                        "current_volatility": round(current_volatility, 4),
                        "historical_volatility": round(historical_volatility, 4),
                        "volatility_ratio": round(volatility_ratio, 2),
                        "threshold": self.VOLATILITY_THRESHOLD,
                        "interpretation": "Increased market uncertainty or regime change"
                    }
                )
                alerts.append(alert)
        
        return alerts
    # ANOMALY DETECTION RULE 3: Rate-of-Change Detection

    def detect_rate_changes(self, current_batch: List[Dict]) -> List[MLAlert]:
        """
        Detect abnormal rate-of-change between batches
        
        Sudden rate changes indicate market shocks or data errors
        """
        alerts = []
        
        if self.previous_batch is None or not current_batch:
            self.previous_batch = current_batch
            return alerts
        
        # Compare last record of previous batch with first of current
        prev_record = self.previous_batch[-1]
        curr_record = current_batch[0]
        
        for tenor in self.TENORS:
            prev_rate = self._safe_float(prev_record.get(tenor))
            curr_rate = self._safe_float(curr_record.get(tenor))
            
            if prev_rate is None or curr_rate is None:
                continue
            
            # Calculate rate change
            rate_change = abs(curr_rate - prev_rate)
            
            if rate_change > self.RATE_CHANGE_THRESHOLD:
                record_date = curr_record.get("record_date", "unknown")
                
                severity = (MLAlertSeverity.CRITICAL if rate_change > 1.0 
                           else MLAlertSeverity.WARNING)
                
                alert = MLAlert(
                    alert_type=MLAlertType.RATE_CHANGE_ANOMALY,
                    severity=severity,
                    title=f"Abnormal Rate Change: {tenor}",
                    message=f"Rate changed {rate_change:.2f}% (from {prev_rate:.2f}% to {curr_rate:.2f}%) on {record_date}",
                    timestamp=datetime.utcnow().isoformat(),
                    record_date=record_date,
                    tenor=tenor,
                    details={
                        "previous_rate": prev_rate,
                        "current_rate": curr_rate,
                        "rate_change_bps": round(rate_change * 100, 2),
                        "threshold_bps": self.RATE_CHANGE_THRESHOLD * 100,
                        "interpretation": "Potential market shock or data quality issue"
                    }
                )
                alerts.append(alert)
        
        self.previous_batch = current_batch
        return alerts
    
    # ANOMALY DETECTION RULE 4: Extreme Value Detection
    
    def detect_extreme_values(self, record: Dict) -> List[MLAlert]:
        """
        Detect rates outside realistic bounds
        
        Extreme values indicate data errors or unprecedented market conditions
        """
        alerts = []
        record_date = record.get("record_date", "unknown")
        
        for tenor in self.TENORS:
            rate = self._safe_float(record.get(tenor))
            if rate is None:
                continue
            
            if rate < self.EXTREME_VALUE_MIN or rate > self.EXTREME_VALUE_MAX:
                alert = MLAlert(
                    alert_type=MLAlertType.EXTREME_VALUE,
                    severity=MLAlertSeverity.CRITICAL,
                    title=f"Extreme Value Detected: {tenor}",
                    message=f"Rate {rate}% is outside realistic bounds [{self.EXTREME_VALUE_MIN}, {self.EXTREME_VALUE_MAX}] on {record_date}",
                    timestamp=datetime.utcnow().isoformat(),
                    record_date=record_date,
                    tenor=tenor,
                    details={
                        "rate": rate,
                        "min_bound": self.EXTREME_VALUE_MIN,
                        "max_bound": self.EXTREME_VALUE_MAX,
                        "interpretation": "Data error or unprecedented market condition"
                    }
                )
                alerts.append(alert)
        
        return alerts
    
    # MASTER DETECTION FUNCTION
    
    def detect_anomalies(self, batch: List[Dict]) -> List[MLAlert]:
        """
        Run all statistical anomaly detection rules on a batch
        
        Args:
            batch: List of Treasury rate records
            
        Returns:
            List of ML anomaly alerts
        """
        all_alerts = []
        
        try:
            # Run per-record detections
            for record in batch:
                all_alerts.extend(self.detect_zscore_anomalies(record))
                all_alerts.extend(self.detect_extreme_values(record))
            
            # Run batch-level detections
            all_alerts.extend(self.detect_volatility_spikes(batch))
            all_alerts.extend(self.detect_rate_changes(batch))
            
            if all_alerts:
                critical_count = sum(1 for a in all_alerts if a.severity == MLAlertSeverity.CRITICAL)
                warning_count = sum(1 for a in all_alerts if a.severity == MLAlertSeverity.WARNING)
                logger.warning(
                    f" ML anomaly detection: {critical_count} critical, "
                    f"{warning_count} warning alerts"
                )
            else:
                logger.info(" No ML anomalies detected")
        
        except Exception as e:
            logger.error(f"ML anomaly detection failed: {e}")
        
        return all_alerts


class DeepMLAnalyzer:
    """
    Tier 2: Deep ML analysis using advanced models
    Called only when Tier 1 detects suspicious patterns
    """
    
    def __init__(self):
        """Initialize deep ML analyzer"""
        logger.info("Deep ML analyzer initialized (placeholder for Snowflake UDFs)")
    
    def analyze_with_isolation_forest(self, records: List[Dict]) -> List[MLAlert]:
        """
        Use Isolation Forest for multivariate anomaly detection
        
        This would call a Snowflake UDF with a trained Isolation Forest model
        For now, this is a placeholder
        """
        alerts = []
        
        # Placeholder: In production, this would call Snowflake external function
        # that runs Isolation Forest on the data
        
        logger.info(" Deep ML: Isolation Forest analysis (placeholder)")
        
        return alerts
    
    def predict_yield_curve(self, historical_data: List[Dict]) -> Dict:
        """
        Use LSTM to predict next yield curve
        
        This would call a Snowflake UDF with a trained LSTM model
        For now, this is a placeholder
        """
        logger.info("🔬 Deep ML: LSTM yield prediction (placeholder)")
        
        # Placeholder for future implementation
        return {
            "predicted_rates": {},
            "confidence_intervals": {},
            "model_version": "v1.0"
        }
    
# HELPER FUNCTIONS

def format_ml_alert_for_slack(alert: MLAlert) -> Dict:
    """Format ML alert for Slack notification"""
    color_map = {
        MLAlertSeverity.INFO: "#36a64f",
        MLAlertSeverity.WARNING: "#ffa500",
        MLAlertSeverity.CRITICAL: "#ff0000",
    }
    
    return {
        "attachments": [
            {
                "color": color_map.get(alert.severity, "#cccccc"),
                "title": f" {alert.severity.value} {alert.title}",
                "text": alert.message,
                "fields": [
                    {
                        "title": "Alert Type",
                        "value": alert.alert_type.value,
                        "short": True
                    },
                    {
                        "title": "Tenor",
                        "value": alert.tenor,
                        "short": True
                    },
                    {
                        "title": "Record Date",
                        "value": alert.record_date,
                        "short": True
                    },
                    {
                        "title": "Details",
                        "value": f"```{alert.details}```",
                        "short": False
                    }
                ],
                "footer": "ML Anomaly Detection System",
                "ts": int(datetime.utcnow().timestamp())
            }
        ]
    }