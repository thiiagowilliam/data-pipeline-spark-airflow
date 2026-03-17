from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.decorators import task

log = logging.getLogger(__name__)

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.540"
)

SPARK_PACKAGES_WITH_BQ = f"{SPARK_PACKAGES},com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"

SPARK_CONF = {
    "spark.pyspark.python": "/usr/bin/python3",
    "spark.pyspark.driver.python": "/usr/bin/python3",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.eventLog.enabled": "false", 
    "spark.dynamicAllocation.enabled": "false",
    "spark.shuffle.service.enabled": "false",
    "spark.executor.instances": "1",
    "spark.executor.cores": "2",
    "spark.executor.memory": "1g",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
}

SPARK_CONF_BQ = SPARK_CONF.copy()
SPARK_CONF_BQ["spark.hadoop.google.cloud.auth.service.account.enable"] = "true"
SPARK_CONF_BQ["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] = "/opt/airflow/dags/credentials/gcp_key.json"

SPARK_JOBS_PATH = "/opt/airflow/dags/scripts/spark_jobs"

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

with DAG(
    dag_id="data_bronze",
    render_template_as_native_obj=True,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "depends_on_past": False,
        "on_failure_callback": on_failure,
    },
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["spark", "minio", "etl", "delta-lake", "bigquery"],
    max_active_runs=1,
    params={
        "MINIO_BUCKET_NAME": "datalake",
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
        sla=timedelta(hours=2),
    )

    raw_to_bronze_delta_clientes_ingestion = SparkSubmitOperator(
        task_id="raw_to_bronze_delta_clientes_ingestion",
        application=f"{SPARK_JOBS_PATH}/bronze_bucket.py",
        conn_id="spark_default",
        name="BronzeForge_DeltaWriter",
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
        application_args=[
            "clientes",
            "s3a://{{ params.MINIO_BUCKET_NAME }}/raw/clientes",
            "s3a://{{ params.MINIO_BUCKET_NAME }}/bronze_layer/clientes",
            "{{ ds }}"
        ],
    )

    validate_bronze_clientes = SparkSubmitOperator(
        task_id="validate_bronze_clientes",
        application=f"{SPARK_JOBS_PATH}/bronze_validation.py",
        conn_id="spark_default",
        name="BronzeForge_Validator",
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
        application_args=[
            "clientes",
            "s3a://{{ params.MINIO_BUCKET_NAME }}/bronze_layer/clientes",
            "{{ ds }}"
        ],
    )

    raw_to_bronze_bigquery_clients_ingestion = SparkSubmitOperator(
        task_id="raw_to_bronze_bigquery_clients_ingestion",
        application=f"{SPARK_JOBS_PATH}/bronze_bigquery.py",
        conn_id="spark_default",
        name="BronzeForge_BigqueryWriter",
        conf=SPARK_CONF_BQ,
        packages=SPARK_PACKAGES_WITH_BQ,
        application_args=[
            "clientes",
            "resolve-ai-407701",
            "s3a://{{ params.MINIO_BUCKET_NAME }}/bronze_layer/clientes",
            "{{ ds }}"
        ],
    )

    @task(task_id="move_raw_to_archive")
    def move_raw_to_archive(bucket_name: str, aws_conn_id: str):
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix="raw/")
        if not keys:
            log.info("Nenhum arquivo na pasta raw/.")
            return

        csv_keys = [k for k in keys if k.endswith('.csv')]
        today = datetime.now().strftime("%Y-%m-%d")

        for key in csv_keys:
            dest_key = key.replace("raw/", f"archives/{today}/", 1)
            s3_hook.copy_object(
                source_bucket_key=key,
                dest_bucket_key=dest_key,
                source_bucket_name=bucket_name,
                dest_bucket_name=bucket_name
            )
            log.info(f"Copiado: {key} → {dest_key}")

            s3_hook.delete_objects(bucket=bucket_name, keys=[key])
            log.info(f"Deletado original: {key}")

    archive_files = move_raw_to_archive(
        bucket_name="{{ params.MINIO_BUCKET_NAME }}",
        aws_conn_id="aws_default"
    )

    wait_for_file >> raw_to_bronze_delta_clientes_ingestion >> validate_bronze_clientes >> raw_to_bronze_bigquery_clients_ingestion >> archive_files