import json
import logging
from datetime import datetime, timezone
import io
import csv
import boto3
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config.config_loader import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_BRONZE_BUCKET,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Expected schema for validation
EXPECTED_COLUMNS = {
    "record_date",
    "maturity_length_months",
    "rate",
    "fetched_at_iso",
}


def _validate_schema(batch: list) -> bool:
    """Validate that batch records have required columns."""
    if not batch:
        return False
    
    actual_columns = set(batch[0].keys())
    missing_columns = EXPECTED_COLUMNS - actual_columns
    
    if missing_columns:
        logger.error(f"Schema validation failed. Missing columns: {missing_columns}")
        return False
    
    logger.info(f"Schema validation passed. Columns: {actual_columns}")
    return True


def _check_file_exists(s3_client, bucket: str, key: str) -> bool:
    """Check if a file already exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except Exception as e:
        logger.error(f"Error checking S3 object: {e}")
        return False


def kafka_to_s3_bronze_pipeline(file_prefix: str):
    """
    Consume a batch of messages from Kafka and write them to S3 Bronze
    as a single CSV file (Federal Credit Similar Maturity Rates).
    
    Args:
        file_prefix: Prefix for the output CSV filename
    """
    consumer = None
    try:
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}")
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Manual commit for idempotency
            consumer_timeout_ms=5000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=KAFKA_GROUP_ID,
        )
        logger.info("Successfully connected to Kafka consumer")

        # Consume batch of messages
        batch = [msg.value for msg in consumer]
        
        if not batch:
            logger.warning("No messages consumed from Kafka; skipping S3 write.")
            return

        logger.info(f"Consumed {len(batch)} messages from Kafka")

        # Validate schema
        if not _validate_schema(batch):
            logger.error("Schema validation failed; aborting S3 write")
            return

        # Extract columns
        columns = sorted(list(batch[0].keys()))
        logger.info(f"Using columns: {columns}")

        # Write to CSV buffer
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=columns)
        writer.writeheader()
        
        for idx, record in enumerate(batch):
            try:
                writer.writerow({col: record.get(col, "") for col in columns})
            except Exception as e:
                logger.error(f"Error writing record {idx}: {e}")
                raise

        csv_body = buffer.getvalue()
        logger.info(f"CSV buffer size: {len(csv_body)} bytes")

        # Setup S3 client
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
        s3 = session.client("s3")
        logger.info(f"Connected to S3 in region {AWS_REGION}")

        # Generate S3 key with date partitioning
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        key = (
            f"federal_maturity_rates/bronze/date={run_date}/"
            f"{file_prefix}_{now_str}.csv"
        )

        # Check if file already exists (idempotency)
        if _check_file_exists(s3, S3_BRONZE_BUCKET, key):
            logger.warning(f"File already exists at {key}; skipping write")
            # Still commit offsets to avoid reprocessing
            consumer.commit()
            return

        # Write to S3
        try:
            logger.info(f"Writing CSV to s3://{S3_BRONZE_BUCKET}/{key}")
            s3.put_object(
                Bucket=S3_BRONZE_BUCKET,
                Key=key,
                Body=csv_body.encode("utf-8"),
                ContentType="text/csv",
            )
            logger.info(f"Successfully wrote CSV file to S3: {key}")

            # Commit offsets only after successful S3 write
            consumer.commit()
            logger.info("Consumer offsets committed successfully")

        except Exception as e:
            logger.error(f"Failed to write to S3: {e}")
            raise

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