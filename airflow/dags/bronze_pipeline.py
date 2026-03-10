from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.540"
)

SPARK_PACKAGES_WITH_JDBC = f"{SPARK_PACKAGES},org.postgresql:postgresql:42.7.2"

SPARK_CONF = {
    "spark.pyspark.python": "/usr/bin/python3",
    "spark.pyspark.driver.python": "/usr/bin/python3",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "file:///opt/spark/spark-events",
}


def on_failure(context: dict) -> None:
    """Log de falha com contexto da task."""
    log.error(
        "Task '%s' falhou no DAG '%s' — execução: %s",
        context["task_instance"].task_id,
        context["dag_run"].dag_id,
        context["dag_run"].run_id,
    )

with DAG(
    dag_id="bronze_data_pipeline",
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
    tags=["spark", "minio", "etl", "delta-lake", "data-quality"],
    max_active_runs=1,
    doc_md="""
    ### Bronze Data Pipeline

    Pipeline ETL que processa dados da camada **raw** para **bronze**:

    | Etapa | Descrição |
    |-------|-----------|
    | **Detecção** | `S3KeySensor` monitora novos CSVs no MinIO |
    | **Filtragem** | Compara com metadata do PostgreSQL para evitar reprocessamento |
    | **Validação** | Valida schema, tipos, nulos e regras de negócio via contratos |
    | **Ingestão** | Converte para Delta Lake e carrega no PostgreSQL |

    **Tecnologias**: Apache Spark, Delta Lake, MinIO (S3), PostgreSQL
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
            doc_md="Aguarda a chegada de novos arquivos CSV no bucket raw do MinIO.",
        )

        list_files = S3ListOperator(
            task_id="list_csv_files",
            bucket="{{ params.minio_bucket_name }}",
            prefix="raw/",
            aws_conn_id="{{ params.minio_conn_id }}",
            doc_md="Lista todos os arquivos CSV disponíveis no bucket raw.",
        )

        @task(doc_md="Consulta metadata de arquivos já processados no PostgreSQL.")
        def get_metadata(postgres_conn_id: str = "postgres_default") -> list[tuple] | None:
            hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            return hook.get_records("SELECT file_key FROM airflow_file_metadata;")

        @task(doc_md="Filtra somente arquivos novos (não processados anteriormente).")
        def select_new_files(
            files: list[str], metadata: list[tuple] | None, bucket_name: str
        ) -> list[str]:
            if not metadata:
                metadata = []
            processed_keys = {row[0] for row in metadata if row}
            new_files = [f for f in files if f not in processed_keys]
            log.info("Arquivos novos encontrados: %d de %d", len(new_files), len(files))
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
        validate_data = SparkSubmitOperator(
            task_id="validate_data",
            application="/opt/airflow/dags/scripts/spark_jobs/validate_data.py",
            conn_id="spark_default",
            name="data-validation-bronze",
            packages=SPARK_PACKAGES,
            conf=SPARK_CONF,
            application_args=new_files_task,
        )

        ingest_to_bronze = SparkSubmitOperator(
            task_id="ingest_to_bronze",
            application="/opt/airflow/dags/scripts/spark_jobs/bronze_ingest.py",
            conn_id="spark_default",
            name="bronze-ingest",
            packages=SPARK_PACKAGES_WITH_JDBC,
            conf=SPARK_CONF,
            application_args=new_files_task,
        )

        validate_data >> ingest_to_bronze

    pre_process >> process_files