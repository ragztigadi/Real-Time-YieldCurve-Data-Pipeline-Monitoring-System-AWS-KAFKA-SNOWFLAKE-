import time
import json
import requests
from kafka import KafkaProducer
from config.config_loader import FINNHUB_API_KEY

API_KEY = FINNHUB_API_KEY
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],  
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_quote(symbol):
    resp = requests.get(BASE_URL, params={"symbol": symbol, "token": API_KEY})
    resp.raise_for_status()
    data = resp.json()
    data["symbol"] = symbol
    return data

def main():
    while True:
        for symbol in SYMBOLS:
            quote = fetch_quote(symbol)
            print("Producing →", quote)
            producer.send("stock-quotes", value=quote)
        time.sleep(1)

if __name__ == "__main__":
    main()
