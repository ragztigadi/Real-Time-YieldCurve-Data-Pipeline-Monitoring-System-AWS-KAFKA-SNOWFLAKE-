import json
import time
import requests
from datetime import datetime

from kafka import KafkaProducer
from config.config_loader import (
    FINNHUB_API_KEY,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    FETCH_INTERVAL_SEC
)
from utils.constants import SYMBOLS, FINNHUB_BASE_URL


def finnhub_to_kafka_pipeline():
    """
    Fetch quotes for SYMBOLS from Finnhub and push to Kafka topic.
    Intended to be called as an Airflow task.
    """
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for symbol in SYMBOLS:
        params = {"symbol": symbol, "token": FINNHUB_API_KEY}
        resp = requests.get(FINNHUB_BASE_URL, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        data["fetched_at_iso"] = datetime.utcnow().isoformat()

        producer.send(KAFKA_TOPIC, value=data)

    producer.flush()
    producer.close()
