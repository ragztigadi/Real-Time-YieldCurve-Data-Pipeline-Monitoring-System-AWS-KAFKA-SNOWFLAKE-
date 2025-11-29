import os
import configparser

config = configparser.ConfigParser()
config.read("config/config.conf")

# --- FEDCREDIT ---
FEDCREDIT_BASE_URL = config["FEDCREDIT"]["BASE_URL"]
FEDCREDIT_ENDPOINT = config["FEDCREDIT"]["ENDPOINT"]
FEDCREDIT_FIELDS = config["FEDCREDIT"]["FIELDS"]
FEDCREDIT_PAGE_SIZE = int(config["FEDCREDIT"]["PAGE_SIZE"])

# --- AWS ---
AWS_ACCESS_KEY_ID = config["AWS"]["AWS_ACCESS_KEY_ID"].strip()
AWS_SECRET_ACCESS_KEY = config["AWS"]["AWS_SECRET_ACCESS_KEY"].strip()
AWS_REGION = config["AWS"]["AWS_REGION"].strip()
S3_BRONZE_BUCKET = config["AWS"]["S3_BRONZE_BUCKET"].strip()

# --- KAFKA ---
# Use environment variable from docker-compose if available, else fall back to config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", config["KAFKA"]["BOOTSTRAP"].strip())
KAFKA_TOPIC = config["KAFKA"]["TOPIC"].strip()
KAFKA_GROUP_ID = config["KAFKA"]["GROUP_ID"].strip()
FETCH_INTERVAL_SEC = int(config["KAFKA"]["FETCH_INTERVAL_SEC"])