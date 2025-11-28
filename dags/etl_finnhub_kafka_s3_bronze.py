import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.finnhub_kafka_pipeline import finnhub_to_kafka_pipeline
from pipelines.kafka_s3_pipeline import kafka_to_s3_bronze_pipeline

default_args = {
    "owner": "codewithRaghav",
    "start_date": datetime(2025, 11, 27),
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id="etl_finnhub_kafka_s3_bronze",
    default_args=default_args,
    schedule_interval="@hourly",  
    catchup=False,
    tags=["finnhub", "kafka", "s3", "bronze"],
)

fetch_and_stream = PythonOperator(
    task_id="finnhub_to_kafka",
    python_callable=finnhub_to_kafka_pipeline,
    dag=dag,
)

kafka_to_s3 = PythonOperator(
    task_id="kafka_to_s3_bronze",
    python_callable=kafka_to_s3_bronze_pipeline,
    op_kwargs={"file_prefix": f"stock_quotes_{file_postfix}"},
    dag=dag,
)

fetch_and_stream >> kafka_to_s3
