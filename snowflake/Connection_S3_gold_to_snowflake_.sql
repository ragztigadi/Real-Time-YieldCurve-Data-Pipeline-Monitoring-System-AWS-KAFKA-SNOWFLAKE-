USE DATABASE TREASURY_DB;
USE SCHEMA RAW_DATA;

-- Check total records loaded
SELECT COUNT(*) as total_records FROM yield_curve_stream;

-- View the latest records with metadata
SELECT 
    record_date,
    record_fiscal_year,
    one_year_or_less,
    between_1_and_5_years,
    between_5_and_10_years,
    ingestion_timestamp,
    kafka_partition,
    stream_offset
FROM yield_curve_stream 
ORDER BY ingestion_timestamp DESC 
LIMIT 10;

-- Check date range
SELECT 
    MIN(record_date) as earliest_date,
    MAX(record_date) as latest_date,
    COUNT(*) as total_records
FROM yield_curve_stream;

-- Validation Results Table
USE DATABASE TREASURY_DB;
USE SCHEMA RAW_DATA;

CREATE OR REPLACE TABLE financial_validation_alerts (
    alert_id VARCHAR(100),
    alert_type VARCHAR(100),
    severity VARCHAR(50),
    title VARCHAR(500),
    message TEXT,
    record_date DATE,
    timestamp TIMESTAMP_NTZ,
    details VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- View financial alerts
SELECT * FROM financial_validation_alerts 
ORDER BY created_at DESC 
LIMIT 20;

-- Count alerts by type
SELECT alert_type, severity, COUNT(*) as count
FROM financial_validation_alerts
GROUP BY alert_type, severity
ORDER BY count DESC;

SELECT 
    alert_type,
    severity,
    title,
    record_date,
    details,
    created_at
FROM financial_validation_alerts
WHERE alert_type = 'butterfly_arbitrage'
ORDER BY created_at DESC
LIMIT 5;

USE DATABASE TREASURY_DB;
USE SCHEMA RAW_DATA;

-- Create table to store ML anomaly alerts
CREATE OR REPLACE TABLE ml_anomaly_alerts (
    alert_id VARCHAR(100),
    alert_type VARCHAR(100),
    severity VARCHAR(50),
    title VARCHAR(500),
    message TEXT,
    record_date DATE,
    tenor VARCHAR(100),
    timestamp TIMESTAMP_NTZ,
    details VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- View all alerts summary
SELECT 'Financial' as source, alert_type, severity, COUNT(*) as count
FROM financial_validation_alerts
GROUP BY alert_type, severity
UNION ALL
SELECT 'ML' as source, alert_type, severity, COUNT(*) as count
FROM ml_anomaly_alerts
GROUP BY alert_type, severity
ORDER BY count DESC;

-- View recent ML alerts
SELECT * FROM ml_anomaly_alerts 
ORDER BY created_at DESC 
LIMIT 10;