"""
Financial Domain Validator for Treasury Yield Curve Data
Implements production-grade financial validations used in risk systems
"""
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class FinancialAlertSeverity(Enum):
    """Financial alert severity levels"""
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class FinancialAlertType(Enum):
    """Types of financial validation alerts"""
    MONOTONICITY_VIOLATION = "monotonicity_violation"
    BUTTERFLY_ARBITRAGE = "butterfly_arbitrage"
    MISSING_TENORS = "missing_tenors"
    NEGATIVE_FORWARD_RATE = "negative_forward_rate"
    LIQUIDITY_PREMIUM_VIOLATION = "liquidity_premium_violation"
    INVERTED_CURVE = "inverted_curve"
    EXTREME_RATE_VALUE = "extreme_rate_value"


@dataclass
class FinancialAlert:
    """Financial validation alert structure"""
    alert_type: FinancialAlertType
    severity: FinancialAlertSeverity
    title: str
    message: str
    timestamp: str
    record_date: str
    details: Dict
    
    def to_dict(self) -> Dict:
        return {
            "type": self.alert_type.value,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp,
            "record_date": self.record_date,
            "details": self.details,
        }


class FinancialValidator:
    """
    Financial domain validator for Treasury yield curve data
    Implements BCBS 239 compliant validation rules
    """
    
    EXPECTED_TENORS = ["one_year_or_less", "between_1_and_5_years", 
                       "between_5_and_10_years", "between_10_and_20_years", 
                       "twenty_years_or_greater"]
    
    TENOR_MIDPOINTS = {
        "one_year_or_less": 0.5,
        "between_1_and_5_years": 3.0,
        "between_5_and_10_years": 7.5,
        "between_10_and_20_years": 15.0,
        "twenty_years_or_greater": 25.0,
    }
    
    # Financial validation thresholds
    BUTTERFLY_SPREAD_MIN = -0.50  # Minimum butterfly spread in % (negative indicates arbitrage)
    LIQUIDITY_PREMIUM_MIN = -0.10  # Minimum spread between tenors
    EXTREME_RATE_MIN = -2.0  # Minimum realistic rate (e.g., negative rates scenario)
    EXTREME_RATE_MAX = 25.0  # Maximum realistic rate
    FORWARD_RATE_MIN = -1.0  # Minimum implied forward rate
    
    def __init__(self):
        """Initialize financial validator"""
        self.alerts: List[FinancialAlert] = []
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _extract_rates(self, record: Dict) -> Dict[str, Optional[float]]:
        """Extract and convert rate values from record"""
        return {
            tenor: self._safe_float(record.get(tenor))
            for tenor in self.EXPECTED_TENORS
        }
    
    # ========================================================================
    # VALIDATION RULE 1: Yield Curve Monotonicity
    # ========================================================================
    
    def validate_monotonicity(self, record: Dict) -> List[FinancialAlert]:
        """
        Validate that yield curve is monotonically increasing
        Longer-term rates should be >= shorter-term rates
        
        Economic principle: Term premium - investors demand higher yields 
        for longer maturity bonds due to increased risk
        """
        alerts = []
        rates = self._extract_rates(record)
        record_date = record.get("record_date", "unknown")
        
        # Filter out None values
        valid_rates = [(tenor, rate) for tenor, rate in rates.items() if rate is not None]
        
        if len(valid_rates) < 2:
            return alerts  # Not enough data to validate
        
        # Check each consecutive pair
        violations = []
        for i in range(len(valid_rates) - 1):
            tenor1, rate1 = valid_rates[i]
            tenor2, rate2 = valid_rates[i + 1]
            
            if rate2 < rate1:
                violations.append({
                    "short_tenor": tenor1,
                    "short_rate": rate1,
                    "long_tenor": tenor2,
                    "long_rate": rate2,
                    "inversion_bps": round((rate1 - rate2) * 100, 2)
                })
        
        if violations:
            alert = FinancialAlert(
                alert_type=FinancialAlertType.MONOTONICITY_VIOLATION,
                severity=FinancialAlertSeverity.WARNING,
                title="Yield Curve Monotonicity Violation",
                message=f"{len(violations)} monotonicity violations detected on {record_date}",
                timestamp=datetime.utcnow().isoformat(),
                record_date=record_date,
                details={
                    "violations": violations,
                    "total_violations": len(violations)
                }
            )
            alerts.append(alert)
        
        return alerts
    
    # ========================================================================
    # VALIDATION RULE 2: Butterfly Spread Arbitrage Detection
    # ========================================================================
    
    def validate_butterfly_spread(self, record: Dict) -> List[FinancialAlert]:
        """
        Validate butterfly spread for arbitrage opportunities
        
        Formula: Butterfly = 2 × Mid_rate - (Short_rate + Long_rate)
        
        A negative butterfly spread indicates potential arbitrage:
        You could buy short + long, sell 2x mid, and lock in risk-free profit
        
        Economic principle: No-arbitrage condition in fixed income markets
        """
        alerts = []
        rates = self._extract_rates(record)
        record_date = record.get("record_date", "unknown")
        
        # Check butterfly spreads across different tenor combinations
        butterflies = []
        
        # Butterfly 1: 1yr, 3yr (mid), 7.5yr
        short = rates.get("one_year_or_less")
        mid = rates.get("between_1_and_5_years")
        long = rates.get("between_5_and_10_years")
        
        if all(r is not None for r in [short, mid, long]):
            butterfly = 2 * mid - (short + long)
            if butterfly < self.BUTTERFLY_SPREAD_MIN:
                butterflies.append({
                    "butterfly_type": "1yr-3yr-7.5yr",
                    "short_rate": short,
                    "mid_rate": mid,
                    "long_rate": long,
                    "butterfly_spread": round(butterfly, 4),
                    "arbitrage_opportunity": True
                })
        
        # Butterfly 2: 3yr, 7.5yr (mid), 15yr
        short = rates.get("between_1_and_5_years")
        mid = rates.get("between_5_and_10_years")
        long = rates.get("between_10_and_20_years")
        
        if all(r is not None for r in [short, mid, long]):
            butterfly = 2 * mid - (short + long)
            if butterfly < self.BUTTERFLY_SPREAD_MIN:
                butterflies.append({
                    "butterfly_type": "3yr-7.5yr-15yr",
                    "short_rate": short,
                    "mid_rate": mid,
                    "long_rate": long,
                    "butterfly_spread": round(butterfly, 4),
                    "arbitrage_opportunity": True
                })
        
        if butterflies:
            alert = FinancialAlert(
                alert_type=FinancialAlertType.BUTTERFLY_ARBITRAGE,
                severity=FinancialAlertSeverity.CRITICAL,
                title="Butterfly Arbitrage Detected",
                message=f"{len(butterflies)} arbitrage opportunities detected on {record_date}",
                timestamp=datetime.utcnow().isoformat(),
                record_date=record_date,
                details={
                    "butterflies": butterflies,
                    "action_required": "Verify data quality or investigate market dislocation"
                }
            )
            alerts.append(alert)
        
        return alerts
    
    # ========================================================================
    # VALIDATION RULE 3: Tenor Completeness
    # ========================================================================
    
    def validate_tenor_completeness(self, record: Dict) -> List[FinancialAlert]:
        """
        Validate that all expected tenors are present and non-null
        
        Missing tenors indicate data quality issues or incomplete market data
        """
        alerts = []
        rates = self._extract_rates(record)
        record_date = record.get("record_date", "unknown")
        
        missing_tenors = [tenor for tenor, rate in rates.items() if rate is None]
        
        if missing_tenors:
            alert = FinancialAlert(
                alert_type=FinancialAlertType.MISSING_TENORS,
                severity=FinancialAlertSeverity.WARNING,
                title="Missing Yield Curve Tenors",
                message=f"{len(missing_tenors)} tenors missing on {record_date}",
                timestamp=datetime.utcnow().isoformat(),
                record_date=record_date,
                details={
                    "missing_tenors": missing_tenors,
                    "expected_tenors": self.EXPECTED_TENORS
                }
            )
            alerts.append(alert)
        
        return alerts
    
    # ========================================================================
    # VALIDATION RULE 4: Forward Rate Consistency
    # ========================================================================
    
    def validate_forward_rates(self, record: Dict) -> List[FinancialAlert]:
        """
        Validate implied forward rates are positive
        
        Formula: Forward rate = (Long_rate × T_long - Short_rate × T_short) / (T_long - T_short)
        
        Negative forward rates indicate market expectations of deflation or data errors
        Economic principle: Expectations hypothesis of the term structure
        """
        alerts = []
        rates = self._extract_rates(record)
        record_date = record.get("record_date", "unknown")
        
        negative_forwards = []
        
        # Calculate forward rates for consecutive tenor pairs
        tenor_pairs = [
            ("one_year_or_less", "between_1_and_5_years"),
            ("between_1_and_5_years", "between_5_and_10_years"),
            ("between_5_and_10_years", "between_10_and_20_years"),
            ("between_10_and_20_years", "twenty_years_or_greater"),
        ]
        
        for short_tenor, long_tenor in tenor_pairs:
            short_rate = rates.get(short_tenor)
            long_rate = rates.get(long_tenor)
            
            if short_rate is not None and long_rate is not None:
                t_short = self.TENOR_MIDPOINTS[short_tenor]
                t_long = self.TENOR_MIDPOINTS[long_tenor]
                
                # Calculate implied forward rate
                forward_rate = (long_rate * t_long - short_rate * t_short) / (t_long - t_short)
                
                if forward_rate < self.FORWARD_RATE_MIN:
                    negative_forwards.append({
                        "short_tenor": short_tenor,
                        "long_tenor": long_tenor,
                        "short_rate": short_rate,
                        "long_rate": long_rate,
                        "forward_rate": round(forward_rate, 4),
                        "period": f"{t_short}y-{t_long}y"
                    })
        
        if negative_forwards:
            alert = FinancialAlert(
                alert_type=FinancialAlertType.NEGATIVE_FORWARD_RATE,
                severity=FinancialAlertSeverity.WARNING,
                title="Negative Implied Forward Rates",
                message=f"{len(negative_forwards)} negative forward rates on {record_date}",
                timestamp=datetime.utcnow().isoformat(),
                record_date=record_date,
                details={
                    "negative_forwards": negative_forwards,
                    "interpretation": "May indicate deflation expectations or data quality issues"
                }
            )
            alerts.append(alert)
        
        return alerts
    
    # ========================================================================
    # VALIDATION RULE 5: Liquidity Premium Validation
    # ========================================================================
    
    def validate_liquidity_premium(self, record: Dict) -> List[FinancialAlert]:
        """
        Validate liquidity premium (term spread) is reasonable
        
        Liquidity premium = Long_rate - Short_rate
        
        Economic principle: Investors demand higher yields for less liquid 
        (longer maturity) bonds. Negative premiums indicate curve inversion.
        """
        alerts = []
        rates = self._extract_rates(record)
        record_date = record.get("record_date", "unknown")
        
        violations = []
        
        # Check key spreads
        spread_checks = [
            ("one_year_or_less", "between_5_and_10_years", "1yr-7.5yr spread"),
            ("between_1_and_5_years", "between_10_and_20_years", "3yr-15yr spread"),
        ]
        
        for short_tenor, long_tenor, spread_name in spread_checks:
            short_rate = rates.get(short_tenor)
            long_rate = rates.get(long_tenor)
            
            if short_rate is not None and long_rate is not None:
                spread = long_rate - short_rate
                
                if spread < self.LIQUIDITY_PREMIUM_MIN:
                    violations.append({
                        "spread_name": spread_name,
                        "short_tenor": short_tenor,
                        "long_tenor": long_tenor,
                        "short_rate": short_rate,
                        "long_rate": long_rate,
                        "spread_bps": round(spread * 100, 2),
                        "inverted": spread < 0
                    })
        
        if violations:
            severity = (FinancialAlertSeverity.CRITICAL 
                       if any(v["inverted"] for v in violations)
                       else FinancialAlertSeverity.WARNING)
            
            alert = FinancialAlert(
                alert_type=FinancialAlertType.LIQUIDITY_PREMIUM_VIOLATION,
                severity=severity,
                title="Liquidity Premium Violation",
                message=f"{len(violations)} liquidity premium violations on {record_date}",
                timestamp=datetime.utcnow().isoformat(),
                record_date=record_date,
                details={
                    "violations": violations,
                    "interpretation": "May indicate yield curve inversion or recession expectations"
                }
            )
            alerts.append(alert)
        
        return alerts
    
    # ========================================================================
    # MASTER VALIDATION FUNCTION
    # ========================================================================
    
    def validate_record(self, record: Dict) -> List[FinancialAlert]:
        """
        Run all financial validation rules on a single record
        
        Args:
            record: Treasury rate record
            
        Returns:
            List of financial alerts (empty if all validations pass)
        """
        all_alerts = []
        
        try:
            # Run all validation rules
            all_alerts.extend(self.validate_monotonicity(record))
            all_alerts.extend(self.validate_butterfly_spread(record))
            all_alerts.extend(self.validate_tenor_completeness(record))
            all_alerts.extend(self.validate_forward_rates(record))
            all_alerts.extend(self.validate_liquidity_premium(record))
            
        except Exception as e:
            logger.error(f"Financial validation failed for record: {e}")
            # Create a generic alert for validation failures
            alert = FinancialAlert(
                alert_type=FinancialAlertType.EXTREME_RATE_VALUE,
                severity=FinancialAlertSeverity.CRITICAL,
                title="Financial Validation Error",
                message=f"Validation failed: {str(e)}",
                timestamp=datetime.utcnow().isoformat(),
                record_date=record.get("record_date", "unknown"),
                details={"error": str(e)}
            )
            all_alerts.append(alert)
        
        return all_alerts
    
    def validate_batch(self, records: List[Dict]) -> List[FinancialAlert]:
        """
        Run financial validations on a batch of records
        
        Args:
            records: List of Treasury rate records
            
        Returns:
            List of all financial alerts
        """
        all_alerts = []
        
        for record in records:
            alerts = self.validate_record(record)
            all_alerts.extend(alerts)
        
        self.alerts.extend(all_alerts)
        
        # Log summary
        if all_alerts:
            critical_count = sum(1 for a in all_alerts if a.severity == FinancialAlertSeverity.CRITICAL)
            warning_count = sum(1 for a in all_alerts if a.severity == FinancialAlertSeverity.WARNING)
            logger.warning(
                f"Financial validation generated {critical_count} critical "
                f"and {warning_count} warning alerts"
            )
        else:
            logger.info("✓ All financial validations passed")
        
        return all_alerts


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def format_financial_alert_for_slack(alert: FinancialAlert) -> Dict:
    """Format financial alert for Slack notification"""
    color_map = {
        FinancialAlertSeverity.INFO: "#36a64f",
        FinancialAlertSeverity.WARNING: "#ffa500",
        FinancialAlertSeverity.CRITICAL: "#ff0000",
    }
    
    return {
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
                "footer": "Financial Risk Alert System",
                "ts": int(datetime.utcnow().timestamp())
            }
        ]
    }