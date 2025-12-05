"""
Kafka to Snowpipe Streaming Pipeline
Real-time row-level ingestion into Snowflake using Snowpipe Streaming API
"""
import json
import logging
from datetime import datetime
from typing import Dict, List
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

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

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SnowpipeStreamingClient:
    """
    Snowpipe Streaming client for real-time data ingestion
    """
    
    def __init__(self):
        """Initialize Snowflake connection"""
        try:
            logger.info(f"Connecting to Snowflake account: {SNOWFLAKE_ACCOUNT}")
            logger.info(f"User: {SNOWFLAKE_USER}")
            logger.info(f"Password length: {len(SNOWFLAKE_PASSWORD) if SNOWFLAKE_PASSWORD else 0}")
            logger.info(f"Warehouse: {SNOWFLAKE_WAREHOUSE}")
            logger.info(f"Database: {SNOWFLAKE_DATABASE}")
            logger.info(f"Schema: {SNOWFLAKE_SCHEMA}")
            
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
            
            # Verify table exists
            self._verify_table()
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def _verify_table(self):
        """Verify target table exists"""
        try:
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
            
        except Exception as e:
            logger.error(f"Table verification failed: {e}")
            raise
    
    def transform_record(self, kafka_record: Dict, partition: int, offset: int) -> Dict:
        """
        Transform Kafka record to Snowflake table schema
        
        Args:
            kafka_record: Raw Kafka message
            partition: Kafka partition number
            offset: Kafka offset
            
        Returns:
            Transformed record matching Snowflake table schema
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
        Insert batch of records into Snowflake using INSERT statement
        
        Args:
            records: List of transformed records
            
        Returns:
            Number of successfully inserted records
        """
        if not records:
            logger.warning("No records to insert")
            return 0
        
        try:
            # Build multi-row INSERT statement
            columns = list(records[0].keys())
            placeholders = ", ".join(["%s"] * len(columns))
            
            insert_query = f"""
            INSERT INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} 
            ({", ".join(columns)})
            VALUES ({placeholders})
            """
            
            # Prepare data for executemany
            data = []
            for record in records:
                row = tuple(record[col] for col in columns)
                data.append(row)
            
            # Execute batch insert
            self.cursor.executemany(insert_query, data)
            self.conn.commit()
            
            inserted_count = len(records)
            logger.info(f"✓ Inserted {inserted_count} records into Snowflake")
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            self.conn.rollback()
            raise
    
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


def kafka_to_snowpipe_streaming_pipeline(batch_size: int = 100, timeout_ms: int = 5000):
    """
    Stream messages from Kafka directly into Snowflake in real-time
    
    Args:
        batch_size: Number of records to batch before inserting
        timeout_ms: Kafka consumer timeout in milliseconds
    """
    consumer = None
    snowflake_client = None
    
    try:
        # Initialize Snowflake client
        logger.info("Initializing Snowflake streaming client...")
        snowflake_client = SnowpipeStreamingClient()
        
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
        total_streamed = 0
        
        for message in consumer:
            try:
                # Transform record
                transformed = snowflake_client.transform_record(
                    kafka_record=message.value,
                    partition=message.partition,
                    offset=message.offset,
                )
                
                batch.append(transformed)
                
                # Insert when batch is full
                if len(batch) >= batch_size:
                    inserted = snowflake_client.insert_batch(batch)
                    total_streamed += inserted
                    
                    # Commit Kafka offsets after successful insert
                    consumer.commit()
                    
                    logger.info(f"Progress: {total_streamed} total records streamed to Snowflake")
                    batch = []
                
            except Exception as e:
                logger.error(f"Failed to process message at offset {message.offset}: {e}")
                continue
        
        # Insert remaining records
        if batch:
            inserted = snowflake_client.insert_batch(batch)
            total_streamed += inserted
            consumer.commit()
        
        logger.info(f"✓ Pipeline completed: {total_streamed} records streamed to Snowflake")
        
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
    # For testing
    kafka_to_snowpipe_streaming_pipeline()