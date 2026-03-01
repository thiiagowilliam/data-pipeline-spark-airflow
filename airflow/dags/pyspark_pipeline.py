import logging
import sqlite3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from scripts.settings import Minio

AIRFLOW_DB_PATH = "/data/metadata.db"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pyspark_pipeline_prod",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["spark", "s3", "etl", "minio"],
    max_active_runs=1,
) as dag:

    with TaskGroup(group_id="pre_process", tooltip="Selecionando novos arquivos") as pre_process:
        wait_for_file = S3KeySensor(
            task_id="wait_for_s3_file",
            bucket_name=Minio.BUCKET_NAME,
            bucket_key="raw/*.csv",
            aws_conn_id=Minio.CONN_ID,
            poke_interval=30,         
            timeout=1800,             
            mode="reschedule",
            wildcard_match=True,
        )

        list_files = S3ListOperator(
            task_id="list_csv_files",
            bucket=Minio.BUCKET_NAME,
            prefix="raw/",
            aws_conn_id=Minio.CONN_ID,
        )

        get_files_metadata = PostgresOperator(
            task_id="get_files_metadata",
            postgres_conn_id="postgres_default",
            sql="SELECT file_key FROM processed_files;",
            do_xcom_push=True, 
        )

        @task(task_id="select_new_files")
        def select_new_files(s3_files, processed_files):
            processed = {row[0] for row in processed_files}
            new_files = [f for f in s3_files if f not in processed]
            print(f"= = = =  {new_files}  = = = =")
            return [f for f in s3_files if f not in processed]

        new_files = select_new_files(
            list_files.output,
            get_files_metadata.output
        )

        @task(task_id="save_new_metadata")
        def save_new_metadata(files):
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            hook = PostgresHook(postgres_conn_id="postgres_default")
            for f in files:
                hook.run(
                    "INSERT INTO processed_files (file_key, processed_at) VALUES (%s, NOW())",
                    parameters=(f,),
                )

        save_task = save_new_metadata(new_files)

    with TaskGroup(group_id="spark_process", tooltip="Processando dados com spark") as spark_process:
        client_bronze_ingest = SparkSubmitOperator(
            task_id="client_bronze_ingest",
            application="local:///app/scripts/bronze_ingest.py",
            conn_id="spark_default",
            deploy_mode="client",      
            py_files="local:///app/deps.zip",
            application_args=[
                "raw/clientes_20260301_001125.csv",
                "raw/vendas_20260301_001125.csv",
            ],
        )
    
    pre_process >> spark_process