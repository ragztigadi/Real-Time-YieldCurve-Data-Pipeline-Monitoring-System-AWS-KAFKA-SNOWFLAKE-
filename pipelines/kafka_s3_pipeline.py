import json
from datetime import datetime, timezone
import io
import csv

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
    as a single CSV file.
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

    # Read all messages currently in the topic
    batch = [msg.value for msg in consumer]
    consumer.close()

    if not batch:
        print("No messages consumed from Kafka; skipping S3 write.")
        return

    # ---- Build CSV in memory ----
    # Use keys from the first record as columns (you can hard-code if you like)
    columns = list(batch[0].keys())

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    for record in batch:
        writer.writerow({col: record.get(col, "") for col in columns})

    csv_body = buffer.getvalue()

    # ---- Write to S3 ----
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3 = session.client("s3")

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    # New CSV prefix under bronze
    key = (
        f"real-time-yieldcurve-bronze/stock_quotes_csv/"
        f"date={run_date}/{file_prefix}_{now_str}.csv"
    )

    s3.put_object(
        Bucket=S3_BRONZE_BUCKET,
        Key=key,
        Body=csv_body.encode("utf-8"),
        ContentType="text/csv",
    )

    print(f"Wrote CSV file to s3://{S3_BRONZE_BUCKET}/{key}")
