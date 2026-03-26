from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.decorators import task

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode

log = logging.getLogger(__name__)

SPARK_PACKAGES = ",".join([
    "io.delta:delta-spark_4.1_2.13:4.1.0",
    "org.apache.hadoop:hadoop-aws:3.4.1",
    "software.amazon.awssdk:bundle:2.30.0"
])

SPARK_PACKAGES_WITH_BQ = (
    f"{SPARK_PACKAGES},"
    "com.google.cloud.spark:spark-4.1-bigquery:0.44.0-preview"
)

SPARK_JARS = "/opt/spark/jars/hadoop-aws-3.4.1.jar,/opt/airflow/jars/aws-sdk-bundle-2.30.0.jar"

# SPARK_CONF = {
#     "spark.jars.ivy": "/tmp/.ivy2",
#     "spark.pyspark.python": "python3",
#     "spark.pyspark.driver.python": "python3",
#     "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
#     "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#     "spark.dynamicAllocation.enabled": "false",
#     "spark.shuffle.service.enabled": "false",

#     "spark.eventLog.enabled": "true",
#     "spark.eventLog.dir": "file:/opt/spark/spark-events",

#     "spark.driver.memory": "512m",       
#     "spark.driver.cores": "1",           
#     "spark.executor.instances": "1",     
#     "spark.executor.cores": "1",         
#     "spark.executor.memory": "512m",     
    
#     "spark.sql.shuffle.partitions": "2", 
#     "spark.default.parallelism": "2",    
    
#     "spark.rpc.message.maxSize": "1024",
#     "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
#     "spark.hadoop.fs.s3a.endpoint.region": "us-east-1", 
#     "spark.hadoop.fs.s3a.access.key": "minioadmin",
#     "spark.hadoop.fs.s3a.secret.key": "minioadmin",
#     "spark.hadoop.fs.s3a.path.style.access": "true",
#     "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
#     "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
#     "spark.sql.legacy.allowUntypedScalaUDF": "true",
#     "spark.databricks.delta.retentionDurationCheck.enabled": "false"
# }


# SPARK_CONF_BQ = SPARK_CONF.copy()
# SPARK_CONF_BQ["spark.hadoop.google.cloud.auth.service.account.enable"] = "true"
# SPARK_CONF_BQ["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] = "/opt/airflow/dags/credentials/gcp_key.json"

SPARK_JOBS_PATH = "/opt/airflow/dags/scripts/spark_jobs"


DBT_DIR = "/opt/airflow/dags/dbt"
DBT_PROFILE_DIR = "/opt/airflow/dags/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"

def on_failure(context: dict) -> None:
    ti = context["task_instance"]
    dag_run = context["dag_run"]
    log.error(
        "[ALERT] Task '%s' falhou | DAG: '%s' | Run: %s | Execução lógica: %s",
        ti.task_id,
        dag_run.dag_id,
        dag_run.run_id,
        context.get("logical_date"),
    )

def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    log.warning(
        "[SLA MISS] DAG '%s' — Tasks que violaram SLA: %s",
        dag.dag_id,
        [str(s) for s in slas],
    )

DBT_DIR = "/opt/airflow/dags/dbt"
DBT_PROFILE_DIR = "/opt/airflow/dags/dbt"

with DAG(
    dag_id="etl_pipeline",
    render_template_as_native_obj=True,
    default_args = {
        'owner': 'thiago',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(hours=2),
    },
    start_date=datetime(2023, 1, 1),
    schedule="@continuous",
    catchup=False,
    tags=["spark", "minio", "etl", "delta-lake", "bigquery"],
    max_active_runs=1,
    params={
        "MINIO_BUCKET_NAME": "datalake",
        "PROJECT_ID": "resolve-ai-407701",
    },
) as dag:

    wait_for_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name="{{ params.MINIO_BUCKET_NAME }}",
        bucket_key="raw/clientes/*.csv",
        aws_conn_id="aws_default",
        wildcard_match=True,
        timeout=3600 * 2,
        mode="reschedule",
        poke_interval=60,
        deferrable=True,
        sla=timedelta(hours=2),
    )

    raw_to_bronze_delta_clientes_ingestion = SparkSubmitOperator(
        task_id="raw_to_bronze_delta_clientes_ingestion",
        application=f"{SPARK_JOBS_PATH}/raw_to_bronze_bucket_ingest.py",
        conn_id="spark_default",
        name="BronzeForge_DeltaWriter",
        # conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
        application_args=[
            "--datalake", "datalake",
            "--layer", "bronze",
            "--table", "clientes",
            "--execution_date", "{{ ds }}",
        ],
    )

    @task(task_id="move_raw_to_archive")
    def move_raw_to_archive(bucket_name: str, aws_conn_id: str, **kwargs):
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        logical_date = kwargs['ds'] 
        
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix="raw/clientes/")
        if not keys:
            logging.info("Nenhum arquivo na pasta raw/clientes/.")
            return

        csv_keys = [k for k in keys if k.endswith('.csv')]
        for key in csv_keys:
            dest_key = key.replace("raw/clientes/", f"archives/clientes/{logical_date}/", 1)
            s3_hook.copy_object(
                source_bucket_key=key,
                dest_bucket_key=dest_key,
                source_bucket_name=bucket_name,
                dest_bucket_name=bucket_name
            )
            logging.info(f"Copiado: {key} → {dest_key}")
            s3_hook.delete_objects(bucket=bucket_name, keys=[key])
            logging.info(f"Deletado original: {key}")

    archive_files = move_raw_to_archive(
        bucket_name="{{ params.MINIO_BUCKET_NAME }}",
        aws_conn_id="aws_default"
    )

    bronze_to_silver_bucket_ingest = SparkSubmitOperator(
        task_id="bronze_to_silver_bucket_ingest",
        application=f"{SPARK_JOBS_PATH}/bronze_to_silver_bucket_ingest.py",
        conn_id="spark_default",
        name="SilverForge_Validator",
        # conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
        application_args=[
            "--project_id", "{{ params.PROJECT_ID }}",
            "--datalake", "{{ params.MINIO_BUCKET_NAME }}",
            "--table", "clientes",
            "--execution_date", "{{ ds }}"
        ],
    )

    # bronze_to_silver_bigquery_ingestion = SparkSubmitOperator(
    #     task_id="bronze_to_silver_bigquery_ingestion",
    #     application=f"{SPARK_JOBS_PATH}/silver_ingest_bigquery.py",
    #     conn_id="spark_default",
    #     name="SilverForge_BigqueryWriter",
    #     conf=SPARK_CONF_BQ,
    #     packages=SPARK_PACKAGES_WITH_BQ,
    #     verbose=True,
    #     execution_timeout=timedelta(minutes=10),
    #     application_args=[
    #         "--project_id", "resolve-ai-407701",
    #         "--dataset", "clientes",
    #         "--delta_path", "s3a://{{ params.MINIO_BUCKET_NAME }}/bronze/clientes",
    #         "--execution_date", "{{ ds }}"
    #     ],
    # )

wait_for_file >> raw_to_bronze_delta_clientes_ingestion >> archive_files >> bronze_to_silver_bucket_ingest