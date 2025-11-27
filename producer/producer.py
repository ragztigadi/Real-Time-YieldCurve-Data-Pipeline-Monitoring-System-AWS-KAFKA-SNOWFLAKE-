import time
import json
import requests
from kafka import KafkaProducer

from config.config_loader import (
    FINNHUB_API_KEY,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    FETCH_INTERVAL_SEC,
)
from utils.constants import SYMBOLS, FINNHUB_BASE_URL


producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def fetch_quote(symbol):
    params = {
        "symbol": symbol,
        "token": FINNHUB_API_KEY,
    }

    try:
        resp = requests.get(FINNHUB_BASE_URL, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None


def main():
    while True:
        for symbol in SYMBOLS:
            quote = fetch_quote(symbol)
            if quote:
                print(f"Producing → {quote}")
                producer.send(KAFKA_TOPIC, value=quote)
        time.sleep(FETCH_INTERVAL_SEC)


if __name__ == "__main__":
    main()
