## 📋 Overview

An end-to-end real-time data pipeline that fetches live stock market data from Finnhub API, processes it through Kafka, implements a medallion architecture (Bronze → Silver → Gold) in AWS S3, catalogs with AWS Glue, loads into Snowflake, and sends Slack alerts for monitoring.

**Key Features:**
- ⚡ Real-time streaming with Kafka
- 🏗️ Medallion Architecture (Bronze/Silver/Gold)
- 📊 Orchestration with Airflow
- ☁️ AWS S3 Data Lake
- ❄️ Snowflake Data Warehouse
- 🚨 Slack Alert System

---

## 🏛️ Architecture

### **Image 1: System Architecture**
![Architecture](docs/images/architecture.png)

**Architecture Components:**

```
Data Source (Finnhub API)
    ↓
Airflow + Kafka (Orchestration & Streaming)
    ↓
S3 Bronze (Raw JSON)
    ↓
AWS Glue Crawler (Schema Detection)
    ↓
AWS Glue ETL (Transformation)
    ↓
S3 Silver (Cleaned Parquet)
    ↓
S3 Gold (Aggregated by Symbol)
    ↓
Snowflake (Data Warehouse)
    ↓
PowerBI/Tableau/QuickSight (Visualization)
    +
Splunk/Slack (Alerts & Monitoring)
```

**Key Components:**
- **Ingestion**: Airflow DAGs fetch data hourly from Finnhub
- **Streaming**: Kafka handles real-time message processing
- **Storage**: S3 medallion architecture for data maturity
- **Cataloging**: AWS Glue auto-discovers schemas
- **Warehousing**: Snowflake for analytics
- **Security**: AWS IAM for access control
- **Monitoring**: Splunk for logs, Slack for alerts

---

## 🔄 Pipeline Walkthrough

### **Image 2: Airflow DAG - Bronze Layer**
![Airflow Bronze DAG](docs/images/airflow-bronze-dag.png)

**What's Happening:**
- DAG: `etl_federalcredit_kafka_s3_bronze`
- **Task 1** (`federalcredit_to_kafka`): Fetches stock data from Finnhub API
- **Task 2** (`kafka_to_s3_bronze`): Consumes Kafka messages → writes to S3 Bronze
- Schedule: Runs hourly to continuously ingest data
- Status: Both tasks completed successfully (green)

---

### **Image 3: Kafka Messages**
![Kafka Messages](docs/images/kafka-messages.png)

**Real-Time Streaming:**
- Topic: `stock-quotes`
- Messages contain OHLC data:
  - `c`: Current price
  - `h`: High, `l`: Low, `o`: Open
  - `dp`: Percent change
  - `symbol`: Stock ticker (AAPL, MSFT, TSLA, GOOGL)
  - `t`: Unix timestamp
- Multiple partitions (0-8) for parallel processing
- Timestamps show microsecond precision

---

### **Image 4: S3 Bucket Structure**
![S3 Buckets](docs/images/s3-buckets.png)

**Medallion Architecture:**
```
real-time-yieldcurve-s3/
├── real-time-yieldcurve-bronze/   (Raw JSON)
├── real-time-yieldcurve-silver/   (Cleaned Parquet)
└── real-time-yieldcurve-gold/     (Aggregated)
```

Three layers implement data maturity stages for progressive refinement.

---

### **Image 5: Bronze Layer - Date Partition**
![Bronze Date Partition](docs/images/bronze-partition.png)

**Bronze Storage:**
- Path: `bronze/stock_quotes/date=2025-11-28/`
- File: `stock_quotes_20251128_20251128T140027.json`
- Format: Raw JSON (1011.0 B)
- Contains all stock quotes fetched in that hour
- Partitioned by date for efficient querying

---

### **Image 6: Bronze Data Sample**
![Bronze JSON](docs/images/bronze-json.png)

**Raw Data Structure:**
```json
{
  "c": 277.55,
  "d": 0.58,
  "dp": 0.2094,
  "h": 279.53,
  "l": 276.63,
  "o": 276.96,
  "pc": 276.97,
  "t": 1764190800,
  "symbol": "AAPL",
  "fetched_at": "2025-11-28T14:00:08.525325"
}
```

Unprocessed data preserving original API response for audit and replay.

---

### **Image 7: AWS Glue Crawler**
![Glue Crawler](docs/images/glue-crawler.png)

**Schema Discovery:**
- Crawler: `rt_yieldcurve_bronze_crawler`
- Database: `rt_yieldcurve_db`
- Status: Completed successfully
- Duration: 41 seconds
- Result: 1 table created, 1 partition detected
- Automatically infers schema from Bronze JSON files

---

### **Image 8: AWS Glue ETL Job**
![Glue ETL](docs/images/glue-etl.png)

**Visual ETL Pipeline:**
- Job: `AWS_Glue_Reddit_ETL`
- **Source**: S3 Bronze bucket (raw data)
- **Transform**: Change Schema (data cleaning, type casting)
- **Target**: S3 Silver bucket (transformed parquet)

**Transformations Applied:**
- Null handling (drop rows with missing symbol/price)
- Data type casting (strings → floats/integers)
- Deduplication (keep latest timestamp)
- Add derived columns (price_range, volatility_pct)

---

### **Image 9: Silver Layer - Parquet Files**
![Silver Parquet](docs/images/silver-parquet.png)

**Cleaned Data:**
- Path: `silver/stock_quotes/`
- Format: Parquet (columnar, compressed)
- Files: Multiple runs with timestamps
- Size: 2.3-3.3 KB per file
- Optimized for analytical queries
- Schema-validated and quality-checked

---

### **Image 10: Slack Alert Integration**
![Slack Alerts](docs/images/slack-alerts.png)

**Alert System:**
- Channel: `#all-treasury-alerts`
- Integration: Treasury Alerts app connected
- Message: "Treasury Alert System Connected!"
- Sends real-time notifications for:
  - Pipeline failures
  - Data quality issues
  - Threshold violations
  - Processing completions

---

### **Image 11: Silver Layer - Multiple Files**
![Silver Files](docs/images/silver-files-list.png)

**Silver Storage Evolution:**
- Multiple parquet files accumulating over time
- Each file represents a processing run
- Incremental data loading pattern
- Partitioned for efficient querying
- Ready for aggregation into Gold layer

---

### **Image 12: Gold Layer - Symbol Partitions**
![Gold Partitions](docs/images/gold-partitions.png)

**Aggregated Data:**
```
gold/
├── symbol=__HIVE_DEFAULT_PARTITION__/
├── symbol=AAPL/
├── symbol=AMZN/
├── symbol=GOOGL/
├── symbol=MSFT/
└── symbol=TSLA/
```

- Partitioned by stock symbol for fast symbol-specific queries
- Business-ready aggregated metrics
- Optimized for BI tool consumption

---

### **Image 13: Snowflake External Stage**
![Snowflake Stage](docs/images/snowflake-stage.png)

**Data Warehouse Integration:**
- External Stage: `s3_treasury_stage`
- Storage Integration: `s3_treasury_integration`
- Source: S3 Silver layer
- File Format: Parquet
- IAM Role: Secure cross-account access
- Query: Creates stage pointing to Silver data

**SQL:**
```sql
CREATE OR REPLACE STAGE s3_treasury_stage
  STORAGE_INTEGRATION = s3_treasury_integration
  URL = 's3://real-time-yieldcurve-s3/silver/treasury-silver/'
  FILE_FORMAT = (TYPE = 'PARQUET');
```

Stage successfully created for data loading.

---

### **Image 14: Slack Webhook Configuration**
![Slack Webhook](docs/images/slack-webhook.png)

**Webhook Setup:**
- Incoming Webhooks: Enabled
- Workspace: Treasury Alerts
- Channel: `#all-treasury-alerts`
- Webhook URL: `https://hooks.slack.com/services/...`
- Added by: ragz.tech22 (Nov 30, 2025)
- Allows programmatic message posting from pipeline

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Data Source | Finnhub API |
| Orchestration | Apache Airflow |
| Streaming | Apache Kafka |
| Storage | AWS S3 |
| Cataloging | AWS Glue |
| ETL | AWS Glue ETL |
| Warehouse | Snowflake |
| Alerts | Slack Webhooks |
| Monitoring | Splunk |
| BI Tools | PowerBI, Tableau, QuickSight |
| Language | Python 3.x |

---

## 📦 Prerequisites

```bash
# Software
- Python 3.8+
- Docker & Docker Compose
- AWS CLI
- Snowflake account
- Finnhub API key
- Slack workspace

# Python packages
pip install apache-airflow kafka-python boto3 pandas pyarrow snowflake-connector-python
```
---

## 📁 Project Structure

Real-Time-YieldCurve-Data-Pipeline/
├── artifacts/
│   ├── federal_credit_maturity_rates_20251129_20251129T015345.csv
│   └── stock_quotes_20251128_20251128T140027.json
├── config/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── config_loader.py
│   └── config.conf
├── dags/
│   ├── __pycache__/
│   └── etl_finnhub_kafka_s3_bronze.py
├── doccuments/
├── logs/
├── pipelines/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── federalcredit_kafka_pipeline.py
│   ├── kafka_s3_pipeline.py
│   └── bronze_to_silver_pipeline.py
├── plugins/
├── producer/
├── utils/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── alert_manager.py
│   └── constants.py
├── venv/
├── .env
├── .gitignore
├── .gitkeep
├── docker-compose.yaml
├── README.md
└── requirements.txt

---

## 🔍 Data Flow Summary

1. **Ingestion**: Airflow fetches Finnhub data hourly
2. **Streaming**: Kafka processes messages in real-time
3. **Bronze**: Raw JSON stored in S3 (date partitioned)
4. **Cataloging**: Glue Crawler detects schema
5. **Transformation**: Glue ETL cleans & validates
6. **Silver**: Parquet files in S3 (quality-checked)
7. **Gold**: Aggregated by symbol in S3
8. **Warehouse**: Snowflake loads via external stage
9. **Analytics**: BI tools query Snowflake
10. **Monitoring**: Slack alerts on issues

---

## 🚨 Alert Types

- ✅ Pipeline success notifications
- ❌ Task failure alerts
- ⚠️ Data quality warnings
- 📊 Threshold violations (price spikes)
- 🔄 Processing completion updates

---

## 📊 Key Transformations

**Bronze → Silver:**
- Null handling (drop invalid records)
- Data type casting
- Deduplication (latest timestamp)
- Derived columns (volatility, direction)
- Format conversion (JSON → Parquet)

**Silver → Gold:**
- Symbol-based aggregation
- Time-window calculations
- Business metrics computation

---

## 🤝 Contributing

Contributions welcome! Please open issues or submit PRs.

---

## 📄 License

MIT License - see LICENSE file for details.

---

## 👤 Author

**Raghav Tigadi**
- GitHub: [@ragztigadi](https://github.com/ragztigadi)

---

## 🙏 Acknowledgments

- Finnhub for market data API and US Department of Treasury for Yield Data 
- Apache Foundation for Kafka & Airflow
- AWS for cloud infrastructure
- Snowflake for data warehousing

---
