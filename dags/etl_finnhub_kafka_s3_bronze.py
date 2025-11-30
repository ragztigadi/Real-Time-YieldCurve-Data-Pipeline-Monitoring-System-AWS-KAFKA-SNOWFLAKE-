import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.federalcredit_kafka_pipeline import federalcredit_to_kafka_pipeline
from pipelines.kafka_s3_pipeline import kafka_to_s3_bronze_pipeline

# DAG configuration
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2024, 1, 1),  
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# DAG definition
dag = DAG(
    dag_id="etl_federalcredit_kafka_s3_bronze",
    default_args=default_args,
    description="Real-time Federal Credit Maturity Rates: Treasury API → Kafka → S3 Bronze",
    schedule_interval="@hourly",  
    catchup=False,
    tags=["federalcredit", "treasury", "kafka", "s3", "bronze", "data-engineering"],
    max_active_runs=1,  
)

file_postfix = "{{ ds_nodash }}_{{ execution_date.strftime('%H%M%S') }}"

# Task 1: Fetch Treasury API → Produce to Kafka
fetch_and_stream_task = PythonOperator(
    task_id="federalcredit_to_kafka",
    python_callable=federalcredit_to_kafka_pipeline,
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

# Task 2: Consume from Kafka → Write to S3 Bronze
kafka_to_s3_task = PythonOperator(
    task_id="kafka_to_s3_bronze",
    python_callable=kafka_to_s3_bronze_pipeline,
    op_kwargs={"file_prefix": f"federal_credit_maturity_rates_{file_postfix}"},
    dag=dag,
    execution_timeout=timedelta(minutes=10),
    depends_on_past=False,
)

# Task dependency: API → Kafka → S3
fetch_and_stream_task >> kafka_to_s3_task