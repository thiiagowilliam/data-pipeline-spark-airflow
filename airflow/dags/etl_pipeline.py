from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

log = logging.getLogger(__name__)

SPARK_PACKAGES = ",".join([
    "io.delta:delta-spark_4.1_2.13:4.1.0",
    "org.apache.hadoop:hadoop-aws:3.4.1",
    "software.amazon.awssdk:bundle:2.30.0"
])
CONF_SPARK = {
        "spark.hadoop.fs.s3a.endpoint": "{{ conn.aws_default.extra_dejson.endpoint_url }}", 
        "spark.hadoop.fs.s3a.access.key": "{{ conn.aws_default.extra_dejson.aws_access_key }}", 
        "spark.hadoop.fs.s3a.secret.key": "{{ conn.aws_default.extra_dejson.aws_secret_key }}"
        }

SPARK_JOBS_PATH = "/opt/airflow/dags/scripts/spark_jobs"

def on_failure(context: dict) -> None:
    ti = context["task_instance"]
    dag_run = context["dag_run"]
    log.error(
        "[ALERT] Task '%s' falhou | DAG: '%s' | Run: %s | Execução lógica: %s",
        ti.task_id, dag_run.dag_id, dag_run.run_id, context.get("logical_date"),
    )

with DAG(
    dag_id="etl_clientes_medallion",
    render_template_as_native_obj=True,
    default_args={
        'owner': 'engenharia_de_dados',
        'depends_on_past': False,
        'on_failure_callback': on_failure,
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
        'execution_timeout': timedelta(hours=1),
    },
    start_date=datetime(2023, 1, 1),
    schedule="*/15 * * * *", 
    catchup=False,
    tags=["spark", "minio", "medallion", "delta"],
    max_active_runs=1,
    params={
        "MINIO_BUCKET_NAME": "datalake",
        "PROJECT_ID": "resolve-ai-407701",
    },
) as dag:
    start = EmptyOperator(task_id="start")

    raw_to_bronze = SparkSubmitOperator(
        task_id="raw_to_bronze_ingestion",
        application=f"{SPARK_JOBS_PATH}/raw_to_bronze_bucket_ingest.py",
        conn_id="spark_default",
        name="Bronze_Ingestion_{{ ds }}",
        packages=SPARK_PACKAGES,
        conf=CONF_SPARK,
        application_args=[
            "--datalake", "{{ params.MINIO_BUCKET_NAME }}",
            "--layer", "bronze",
            "--table", "clientes",
            "--execution_date", "{{ ds }}",
        ]
    )
    
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver_quality",
        application=f"{SPARK_JOBS_PATH}/bronze_to_silver_bucket_ingest.py",
        conn_id="spark_default",
        name="Silver_Quality_{{ ds }}",
        packages=SPARK_PACKAGES,
        conf=CONF_SPARK,
        application_args=[
            "--project_id", "{{ params.PROJECT_ID }}",
            "--datalake", "{{ params.MINIO_BUCKET_NAME }}",
            "--table", "clientes",
            "--execution_date", "{{ ds }}"
        ],
    )

    @task(task_id="move_to_archive")
    def move_to_archive_task(**context):
        hook = S3Hook(aws_conn_id="aws_default")
        bucket = context["params"]["MINIO_BUCKET_NAME"]
        table = "clientes"
        source_prefix = f"raw/{table}/"
        archive_prefix = f"archives/{table}/"
        
        log.info(f"Listando arquivos em: s3://{bucket}/{source_prefix}")
        keys = hook.list_keys(bucket_name=bucket, prefix=source_prefix)
        
        if not keys:
            log.info("Nenhum arquivo encontrado para mover.")
            return

        for key in keys:
            if key.endswith('/'):
                continue

            filename = key.split('/')[-1]
            dest_key = f"{archive_prefix}{filename}"
            
            log.info(f"Movendo {key} para {dest_key}")
            hook.copy_object(
                source_bucket_key=key,
                dest_bucket_key=dest_key,
                source_bucket_name=bucket,
                dest_bucket_name=bucket
            )
            hook.delete_objects(bucket=bucket, keys=key)
            
        log.info(f"Sucesso: {len(keys)} arquivos movidos para archives.")

    move_archive = move_to_archive_task()
    
    end = EmptyOperator(task_id="end")

    start >> raw_to_bronze >> [move_archive, bronze_to_silver] >> end