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
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", config.get("KAFKA", "bootstrap_servers").strip())
KAFKA_TOPIC = config["KAFKA"]["TOPIC"].strip()
KAFKA_GROUP_ID = config["KAFKA"]["GROUP_ID"].strip()
FETCH_INTERVAL_SEC = int(config["KAFKA"]["FETCH_INTERVAL_SEC"])

# --- SNOWFLAKE -----
# Snowflake Configuration
SNOWFLAKE_ACCOUNT = config.get("snowflake", "account").strip().strip('"').strip("'")
SNOWFLAKE_USER = config.get("snowflake", "user").strip().strip('"').strip("'")
SNOWFLAKE_PASSWORD = config.get("snowflake", "password").strip().strip('"').strip("'")
SNOWFLAKE_WAREHOUSE = config.get("snowflake", "warehouse").strip().strip('"').strip("'")
SNOWFLAKE_DATABASE = config.get("snowflake", "database").strip().strip('"').strip("'")
SNOWFLAKE_SCHEMA = config.get("snowflake", "schema").strip().strip('"').strip("'")
SNOWFLAKE_ROLE = config.get("snowflake", "role").strip().strip('"').strip("'")
SNOWFLAKE_TABLE = config.get("snowflake", "table").strip().strip('"').strip("'")

# ---Slak Alert --- 
SLACK_WEBHOOK_URL = config.get("slack", "webhook_url", fallback="")