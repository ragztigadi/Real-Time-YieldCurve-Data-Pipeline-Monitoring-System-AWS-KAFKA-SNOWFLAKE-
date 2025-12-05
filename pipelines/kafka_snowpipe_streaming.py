"""
Kafka to Snowpipe Streaming Pipeline with Financial Validations
Real-time row-level ingestion into Snowflake with production-grade financial validation
"""
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, List
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import snowflake.connector

from config.config_loader import (
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_TABLE,
)
from utils.financial_validator import FinancialValidator, FinancialAlert, format_financial_alert_for_slack
from utils.alert_manager import AlertManager

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SnowpipeStreamingClient:
    """
    Snowpipe Streaming client with financial validation
    """
    
    def __init__(self):
        """Initialize Snowflake connection"""
        try:
            logger.info(f"Connecting to Snowflake account: {SNOWFLAKE_ACCOUNT}")
            
            self.conn = snowflake.connector.connect(
                account=SNOWFLAKE_ACCOUNT,
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                role=SNOWFLAKE_ROLE,
            )
            
            self.cursor = self.conn.cursor()
            logger.info("✓ Successfully connected to Snowflake")
            
            # Verify tables exist
            self._verify_tables()
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def _verify_tables(self):
        """Verify target tables exist"""
        try:
            # Verify main data table
            query = f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{SNOWFLAKE_SCHEMA}' 
            AND table_name = '{SNOWFLAKE_TABLE.upper()}'
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            
            if result[0] == 0:
                logger.error(f"Table {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} does not exist!")
                raise Exception("Target table not found. Please create it first.")
            
            logger.info(f"✓ Target table {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} verified")
            
            # Verify alerts table
            query = f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{SNOWFLAKE_SCHEMA}' 
            AND table_name = 'FINANCIAL_VALIDATION_ALERTS'
            """
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            
            if result[0] == 0:
                logger.warning("Financial validation alerts table not found - alerts won't be persisted")
            else:
                logger.info("✓ Financial validation alerts table verified")
            
        except Exception as e:
            logger.error(f"Table verification failed: {e}")
            raise
    
    def transform_record(self, kafka_record: Dict, partition: int, offset: int) -> Dict:
        """
        Transform Kafka record to Snowflake table schema
        """
        try:
            return {
                "RECORD_DATE": kafka_record.get("record_date"),
                "RECORD_FISCAL_YEAR": kafka_record.get("record_fiscal_year"),
                "ONE_YEAR_OR_LESS": self._safe_float(kafka_record.get("one_year_or_less")),
                "BETWEEN_1_AND_5_YEARS": self._safe_float(kafka_record.get("between_1_and_5_years")),
                "BETWEEN_5_AND_10_YEARS": self._safe_float(kafka_record.get("between_5_and_10_years")),
                "BETWEEN_10_AND_20_YEARS": self._safe_float(kafka_record.get("between_10_and_20_years")),
                "TWENTY_YEARS_OR_GREATER": self._safe_float(kafka_record.get("twenty_years_or_greater")),
                "FETCHED_AT_ISO": kafka_record.get("fetched_at_iso"),
                "INGESTION_TIMESTAMP": datetime.utcnow().isoformat(),
                "STREAM_OFFSET": str(offset),
                "KAFKA_PARTITION": partition,
                "KAFKA_TOPIC": KAFKA_TOPIC,
            }
        except Exception as e:
            logger.error(f"Record transformation failed: {e}")
            raise
    
    def _safe_float(self, value) -> float:
        """Safely convert value to float"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def insert_batch(self, records: List[Dict]) -> int:
        """
        Insert batch of records into Snowflake
        """
        if not records:
            logger.warning("No records to insert")
            return 0
        
        try:
            columns = list(records[0].keys())
            placeholders = ", ".join(["%s"] * len(columns))
            
            insert_query = f"""
            INSERT INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} 
            ({", ".join(columns)})
            VALUES ({placeholders})
            """
            
            data = []
            for record in records:
                row = tuple(record[col] for col in columns)
                data.append(row)
            
            self.cursor.executemany(insert_query, data)
            self.conn.commit()
            
            inserted_count = len(records)
            logger.info(f"✓ Inserted {inserted_count} records into Snowflake")
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            self.conn.rollback()
            raise
    
    def insert_validation_alerts(self, alerts: List[FinancialAlert]) -> int:
        """
        Insert financial validation alerts into Snowflake
        """
        if not alerts:
            return 0
        
        try:
            # Insert alerts one by one to avoid batch issues with VARIANT type
            inserted_count = 0
            for alert in alerts:
                alert_id = str(uuid.uuid4())
                
                insert_query = f"""
                INSERT INTO {SNOWFLAKE_SCHEMA}.FINANCIAL_VALIDATION_ALERTS 
                (alert_id, alert_type, severity, title, message, record_date, timestamp, details)
                SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
                """
                
                self.cursor.execute(insert_query, (
                    alert_id,
                    alert.alert_type.value,
                    alert.severity.value,
                    alert.title,
                    alert.message,
                    alert.record_date,
                    alert.timestamp,
                    json.dumps(alert.details)
                ))
                inserted_count += 1
            
            self.conn.commit()
            logger.info(f"✓ Inserted {inserted_count} validation alerts into Snowflake")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Alert insert failed: {e}")
            self.conn.rollback()
            return 0
    
    def close(self):
        """Close Snowflake connection"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Snowflake connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")


def kafka_to_snowpipe_streaming_pipeline(
    batch_size: int = 100, 
    timeout_ms: int = 5000,
    enable_financial_validation: bool = True,
    slack_webhook_url: str = None
):
    """
    Stream messages from Kafka to Snowflake with financial validation
    
    Args:
        batch_size: Number of records to batch before inserting
        timeout_ms: Kafka consumer timeout in milliseconds
        enable_financial_validation: Run financial validations
        slack_webhook_url: Slack webhook for financial alerts
    """
    consumer = None
    snowflake_client = None
    financial_validator = None
    alert_manager = None
    
    try:
        # Initialize Snowflake client
        logger.info("Initializing Snowflake streaming client...")
        snowflake_client = SnowpipeStreamingClient()
        
        # Initialize financial validator
        if enable_financial_validation:
            logger.info("Initializing financial validator...")
            financial_validator = FinancialValidator()
            
            # Initialize alert manager for Slack notifications
            if slack_webhook_url:
                alert_manager = AlertManager(slack_webhook_url=slack_webhook_url)
                logger.info("✓ Slack notifications enabled for financial alerts")
        
        # Initialize Kafka consumer
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}")
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=timeout_ms,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=KAFKA_GROUP_ID,
        )
        logger.info("✓ Successfully connected to Kafka consumer")
        
        # Process messages in batches
        batch = []
        raw_batch = []  # Keep raw records for validation
        total_streamed = 0
        total_alerts = 0
        
        for message in consumer:
            try:
                # Store raw record for validation
                raw_batch.append(message.value)
                
                # Transform record for Snowflake
                transformed = snowflake_client.transform_record(
                    kafka_record=message.value,
                    partition=message.partition,
                    offset=message.offset,
                )
                
                batch.append(transformed)
                
                # Process batch when full
                if len(batch) >= batch_size:
                    # Insert data into Snowflake
                    inserted = snowflake_client.insert_batch(batch)
                    total_streamed += inserted
                    
                    # Run financial validations
                    if enable_financial_validation and financial_validator:
                        logger.info("Running financial validations...")
                        financial_alerts = financial_validator.validate_batch(raw_batch)
                        
                        if financial_alerts:
                            # Store alerts in Snowflake
                            alert_count = snowflake_client.insert_validation_alerts(financial_alerts)
                            total_alerts += alert_count
                            
                            # Send critical alerts to Slack
                            if alert_manager:
                                for alert in financial_alerts:
                                    slack_msg = format_financial_alert_for_slack(alert)
                                    alert_manager.send_slack_notification_raw(slack_msg)
                            
                            # Log summary
                            critical = sum(1 for a in financial_alerts if "CRITICAL" in a.severity.value)
                            warning = sum(1 for a in financial_alerts if "WARNING" in a.severity.value)
                            logger.warning(
                                f"⚠️ Financial validation generated {critical} critical "
                                f"and {warning} warning alerts"
                            )
                        else:
                            logger.info("✓ All financial validations passed")
                    
                    # Commit Kafka offsets after successful processing
                    consumer.commit()
                    
                    logger.info(
                        f"Progress: {total_streamed} records streamed, "
                        f"{total_alerts} financial alerts generated"
                    )
                    
                    # Reset batches
                    batch = []
                    raw_batch = []
                
            except Exception as e:
                logger.error(f"Failed to process message at offset {message.offset}: {e}")
                continue
        
        # Process remaining records
        if batch:
            inserted = snowflake_client.insert_batch(batch)
            total_streamed += inserted
            
            # Validate remaining records
            if enable_financial_validation and financial_validator and raw_batch:
                financial_alerts = financial_validator.validate_batch(raw_batch)
                if financial_alerts:
                    alert_count = snowflake_client.insert_validation_alerts(financial_alerts)
                    total_alerts += alert_count
            
            consumer.commit()
        
        logger.info(
            f"✓ Pipeline completed: {total_streamed} records streamed, "
            f"{total_alerts} financial alerts generated"
        )
        
    except KafkaError as e:
        logger.error(f"Kafka error: {e}")
        raise
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed")
        
        if snowflake_client:
            snowflake_client.close()


if __name__ == "__main__":
    # For testing - set your Slack webhook URL here
    SLACK_WEBHOOK = ""  # Add your Slack webhook URL
    
    kafka_to_snowpipe_streaming_pipeline(
        enable_financial_validation=True,
        slack_webhook_url=SLACK_WEBHOOK
    )