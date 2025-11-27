import configparser

config = configparser.ConfigParser()
config.read("config/config.conf")

FINNHUB_API_KEY = config["DEFAULT"]["FINNHUB_API_KEY"]
KAFKA_BOOTSTRAP = config["DEFAULT"]["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = config["DEFAULT"]["KAFKA_TOPIC"]
FETCH_INTERVAL_SEC = int(config["DEFAULT"]["FETCH_INTERVAL_SEC"])
