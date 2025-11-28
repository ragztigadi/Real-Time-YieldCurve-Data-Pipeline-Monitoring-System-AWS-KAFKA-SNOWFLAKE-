import configparser

config = configparser.ConfigParser()
config.read("config/config.conf")

FINNHUB_API_KEY = config["DEFAULT"]["FINNHUB_API_KEY"]
KAFKA_BOOTSTRAP = config["DEFAULT"]["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = config["DEFAULT"]["KAFKA_TOPIC"]
FETCH_INTERVAL_SEC = int(config["DEFAULT"]["FETCH_INTERVAL_SEC"])

AWS_ACCESS_KEY_ID = config["AWS"]["AWS_ACCESS_KEY_ID"].strip()
AWS_SECRET_ACCESS_KEY = config["AWS"]["AWS_SECRET_ACCESS_KEY"].strip()
AWS_REGION = config["AWS"]["AWS_REGION"].strip()
S3_BRONZE_BUCKET = config["AWS"]["S3_BRONZE_BUCKET"].strip()
