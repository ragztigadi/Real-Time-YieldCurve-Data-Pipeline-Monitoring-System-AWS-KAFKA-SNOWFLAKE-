import os
from utils.alert_manager import AlertManager, AlertType, AlertSeverity, Alert
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

# Alert configuration (read from environment)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")


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


def _read_previous_batch() -> list:
    """
    Read the previous batch from S3 for yield jump comparison.
    This enables anomaly detection across batches.
    """
    try:
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
        s3 = session.client("s3")
        
        # List all files in Bronze zone, sorted by date
        response = s3.list_objects_v2(
            Bucket=S3_BRONZE_BUCKET,
            Prefix="federal_maturity_rates/bronze/",
            MaxKeys=10
        )
        
        if "Contents" not in response or len(response["Contents"]) < 2:
            logger.info("No previous batch available for comparison")
            return []
        
        # Get second-to-last file (last is current)
        previous_file = sorted(response["Contents"], key=lambda x: x["LastModified"])[-2]
        previous_key = previous_file["Key"]
        
        logger.info(f"Reading previous batch from {previous_key}")
        
        obj = s3.get_object(Bucket=S3_BRONZE_BUCKET, Key=previous_key)
        csv_content = obj["Body"].read().decode("utf-8")
        
        reader = csv.DictReader(io.StringIO(csv_content))
        previous_records = list(reader)
        
        logger.info(f"Loaded {len(previous_records)} records from previous batch")
        return previous_records
        
    except Exception as e:
        logger.warning(f"Could not read previous batch: {e}")
        return []


def kafka_to_s3_bronze_pipeline(file_prefix: str):
    """
    Consume a batch of messages from Kafka, validate them, trigger alerts,
    and write them to S3 Bronze as a single CSV file.
    
    Args:
        file_prefix: Prefix for the output CSV filename
    """
    consumer = None
    
    # Initialize alert manager
    alert_manager = AlertManager(
        slack_webhook_url=SLACK_WEBHOOK_URL,
        sns_topic_arn=SNS_TOPIC_ARN,
    )
    
    try:
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}")
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            auto_offset_reset="earliest",
            enable_auto_commit=False,  
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
            # Send critical alert for schema failure
            alert = Alert(
                alert_type=AlertType.SCHEMA_ERROR,
                severity=AlertSeverity.CRITICAL,
                title="Schema Validation Failed",
                message="Kafka batch has missing or unexpected columns",
                timestamp=datetime.utcnow().isoformat(),
                details={"batch_size": len(batch), "expected_columns": list(EXPECTED_COLUMNS)}
            )
            alert_manager.notify(alert)
            return

        logger.info("Running data quality validations and alert checks...")
        
        # Run all validation checks on current batch
        validation_alerts = alert_manager.validate_batch(batch)
        
        # Check for yield jumps (requires previous batch)
        previous_batch = _read_previous_batch()
        if previous_batch:
            jump_alerts = alert_manager.check_yield_jumps(previous_batch, batch)
            validation_alerts.extend(jump_alerts)
        
        # Send all alerts to Slack/SNS
        alert_manager.notify_all(validation_alerts)
        
        # Log alert summary
        if validation_alerts:
            critical_alerts = [a for a in validation_alerts if a.severity == AlertSeverity.CRITICAL]
            warning_alerts = [a for a in validation_alerts if a.severity == AlertSeverity.WARNING]
            logger.warning(f"Validation generated {len(critical_alerts)} critical and {len(warning_alerts)} warning alerts")
        else:
            logger.info(" All validations passed - no alerts triggered")

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

        if _check_file_exists(s3, S3_BRONZE_BUCKET, key):
            logger.warning(f"File already exists at {key}; skipping write")
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
            logger.info(f"✅ Successfully wrote CSV file to S3: {key}")

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