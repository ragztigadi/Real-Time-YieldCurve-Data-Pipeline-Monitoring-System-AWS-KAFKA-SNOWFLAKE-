import time
import json
import requests
import pandas as pd
from kafka import KafkaProducer

from utils.constants import (
    FEDCREDIT_BASE_URL,
    FEDCREDIT_ENDPOINT,
    FEDCREDIT_FIELDS,
    FEDCREDIT_PAGE_SIZE,
)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def build_initial_url():
    base = FEDCREDIT_BASE_URL.rstrip("/") + FEDCREDIT_ENDPOINT
    params = {
        "format": "json",
        "page[size]": str(FEDCREDIT_PAGE_SIZE),
    }
    fields = FEDCREDIT_FIELDS.strip()
    if fields:
        params["fields"] = fields
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{base}?{query}"

def fetch_all_records():
    all_rows = []
    next_url = build_initial_url()

    while next_url:
        resp = requests.get(next_url, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", [])
        if not data:
            break
        all_rows.extend(data)
        links = payload.get("links", {})
        next_url = links.get("next")

    return all_rows

def main():
    while True:
        rows = fetch_all_records()
        for row in rows:
            print("Producing →", row)
            producer.send("federal-credit-rates", value=row)
        time.sleep(60)  # adjust as needed

if __name__ == "__main__":
    main()
