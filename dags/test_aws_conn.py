from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator 
import logging

log = logging.getLogger(__name__)

with DAG(
    dag_id="test_aws_conn",
    description="Simple DAG to verify AWS S3 connection credentials",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aws", "s3", "connection", "test"],
) as dag:

    list_s3_objects = S3ListOperator(
        task_id="list_s3_objects",
        aws_conn_id="aws_default",
        bucket="rbf-daily-stocks-data-am1",
    )

    def log_success(**context):
        log.info("✅ AWS connection successful")

    log_success_task = PythonOperator(
        task_id="log_success",
        python_callable=log_success,
    )

    list_s3_objects >> log_success_task
