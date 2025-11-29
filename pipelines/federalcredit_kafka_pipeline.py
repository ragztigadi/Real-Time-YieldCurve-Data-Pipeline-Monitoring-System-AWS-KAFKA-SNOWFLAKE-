import json
import logging
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.config_loader import (
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    FETCH_INTERVAL_SEC,
)
from utils.constants import (
    FEDCREDIT_BASE_URL,
    FEDCREDIT_ENDPOINT,
    FEDCREDIT_FIELDS,
    FEDCREDIT_PAGE_SIZE,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _build_initial_url() -> str:
    """Build the initial API URL with pagination and field parameters."""
    base = FEDCREDIT_BASE_URL.rstrip("/") + FEDCREDIT_ENDPOINT
    params = {
        "format": "json",
        "page[size]": str(FEDCREDIT_PAGE_SIZE),
    }
    fields = FEDCREDIT_FIELDS.strip()
    if fields:
        params["fields"] = fields
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{base}?{query}"
    logger.info(f"Built initial URL: {url}")
    return url


def _fetch_all_records():
    """Fetch all Federal Credit Similar Maturity Rates from Treasury API with pagination."""
    all_rows = []
    next_url = _build_initial_url()
    page_count = 0

    while next_url:
        try:
            logger.info(f"Fetching page {page_count + 1} from {next_url}")
            resp = requests.get(next_url, timeout=60)
            resp.raise_for_status()
            payload = resp.json()

            data = payload.get("data", [])
            if not data:
                logger.info("No more data available; stopping pagination")
                break

            all_rows.extend(data)
            logger.info(f"Page {page_count + 1}: Fetched {len(data)} records (Total: {len(all_rows)})")
            
            links = payload.get("links", {})
            next_url = links.get("next")
            page_count += 1

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            raise

    logger.info(f"Fetched total {len(all_rows)} records from Treasury API")
    return all_rows


def _validate_record(record: dict) -> bool:
    """Validate that a record has required fields."""
    required_fields = {"record_date", "maturity_length_months", "rate"}
    return all(field in record for field in required_fields)


def federalcredit_to_kafka_pipeline():
    """
    Fetch Federal Credit Similar Maturity Rates from Treasury API
    and push all records to Kafka topic.
    Intended to be called as an Airflow task.
    """
    producer = None
    try:
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}")
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        logger.info("Successfully connected to Kafka")

        # Fetch records from API
        records = _fetch_all_records()
        
        if not records:
            logger.warning("No records fetched from API")
            return

        fetched_at_iso = datetime.utcnow().isoformat()
        sent_count = 0
        failed_count = 0

        for idx, rec in enumerate(records):
            try:
                # Validate record
                if not _validate_record(rec):
                    logger.warning(f"Skipping invalid record at index {idx}: {rec}")
                    failed_count += 1
                    continue

                # Add metadata
                rec["fetched_at_iso"] = fetched_at_iso
                
                # Send to Kafka
                future = producer.send(KAFKA_TOPIC, value=rec)
                future.get(timeout=10)  # Wait for confirmation
                sent_count += 1

                if (sent_count + failed_count) % 100 == 0:
                    logger.info(f"Progress: {sent_count} sent, {failed_count} failed")

            except KafkaError as e:
                logger.error(f"Failed to send record {idx} to Kafka: {e}")
                failed_count += 1
            except Exception as e:
                logger.error(f"Unexpected error processing record {idx}: {e}")
                failed_count += 1

        producer.flush()
        logger.info(f"Pipeline completed: {sent_count} records sent, {failed_count} failed")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed")