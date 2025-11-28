import json
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer
from config.config_loader import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_BRONZE_BUCKET,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
)


def kafka_to_s3_bronze_pipeline(file_prefix: str):
    """
    Consume a batch of messages from Kafka and write them to S3 Bronze
    as a single JSON file.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,  # stop after 5s of no messages
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="airflow-batch-consumer",
    )

    batch = [msg.value for msg in consumer]
    consumer.close()

    if not batch:
        print("No messages consumed from Kafka; skipping S3 write.")
        return

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3 = session.client("s3")

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    key = (
        f"real-time-yieldcurve-bronze/stock_quotes/"
        f"date={run_date}/{file_prefix}_{now_str}.json"
    )

    print("DEBUG S3 bucket:", repr(S3_BRONZE_BUCKET))
    print("DEBUG S3 key:", key)

    s3.put_object(
        Bucket=S3_BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(batch),
        ContentType="application/json",
    )
