from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

def failure_callback(context):
    """Log task failure."""
    log.error(
        "Task %s failed in DAG %s",
        context["task_instance"].task_id,
        context["dag_run"].dag_id,
    )

with DAG(
    dag_id="s3_spark_processing_pipeline",
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        "depends_on_past": False,
        "on_failure_callback": failure_callback,
    },
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["spark", "minio", "etl"],
    max_active_runs=1,
    doc_md="""
    ### S3 Spark Processing Pipeline

    This DAG orchestrates a pipeline that processes files from an S3-compatible storage (MinIO),
    transforms them using Spark, and loads the result into a PostgreSQL database.
    """,
    params={
        "minio_conn_id": Param("aws_default", type="string"),
        "minio_bucket_name": Param("datalake", type="string"),
    },
) as dag:
    with TaskGroup(group_id="pre_process") as pre_process:
        wait_for_file = S3KeySensor(
            task_id="wait_for_s3_file",
            bucket_name="{{ params.minio_bucket_name }}",
            bucket_key="raw/*.csv",
            aws_conn_id="{{ params.minio_conn_id }}",
            wildcard_match=True,
            timeout=3600 * 2,
            mode="reschedule",
        )

        list_files = S3ListOperator(
            task_id="list_csv_files",
            bucket="{{ params.minio_bucket_name }}",
            prefix="raw/",
            aws_conn_id="{{ params.minio_conn_id }}",
        )

        @task
        def get_metadata(postgres_conn_id: str = "postgres_default") -> list[tuple] | None:
            hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            return hook.get_records("SELECT file_key FROM airflow_file_metadata;")

        @task
        def select_new_files(
            files: list[str], metadata: list[tuple] | None, bucket_name: str
        ) -> list[str]:
            if not metadata:
                metadata = []
            metadata_keys = {row[0] for row in metadata if row}
            new_files = [f for f in files if f not in metadata_keys]
            return [f"s3a://{bucket_name}/{file}" for file in new_files]

        metadata_task = get_metadata()
        new_files_task = select_new_files(
            files=list_files.output,
            metadata=metadata_task,
            bucket_name="{{ params.minio_bucket_name }}",
        )

        wait_for_file >> list_files
        list_files >> metadata_task >> new_files_task

    with TaskGroup(group_id="process_files") as process_files:
        process_file = SparkSubmitOperator(
            task_id="run_spark_job",
            application="/opt/airflow/dags/scripts/spark_jobs/bronze_client_ingest.py",
            name="arrow-spark",
            conn_id="spark_default",
            packages="io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.540,org.postgresql:postgresql:42.7.2",
            conf={
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": "file:///opt/spark/spark-events"
            },
            application_args=new_files_task,
        )

    pre_process >> process_files