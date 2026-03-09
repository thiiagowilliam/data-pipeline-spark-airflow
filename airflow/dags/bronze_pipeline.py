from airflow import DAG
from scripts.settings import Minio
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'depends_on_past': False,
}

with DAG(
    dag_id='s3_spark_processing_pipeline',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['spark', 'minio', 'etl'],
    max_active_runs=1,
) as dag:

    with TaskGroup(group_id="pre_process") as pre_process:

        wait_for_file = S3KeySensor(
            task_id='wait_for_s3_file',
            bucket_name=Minio.BUCKET_NAME,
            bucket_key='raw/*.csv',
            aws_conn_id=Minio.CONN_ID,
            wildcard_match=True,
            timeout=3600 * 2,
            mode='reschedule',
        )

        list_files = S3ListOperator(
            task_id='list_csv_files',
            bucket=Minio.BUCKET_NAME,
            prefix='raw/',
            aws_conn_id=Minio.CONN_ID,
        )

        @task(task_id="get_metadata")
        def get_metadata():
            from airflow.providers.postgres.hooks.postgres import PostgresHook
            hook = PostgresHook(postgres_conn_id='postgres_default')
            return hook.get_records("SELECT file_key FROM airflow_file_metadata;")

        metadata = get_metadata()

        @task(task_id="select_new_files")
        def select_new_files(files: list, metadata: list, bucket_name) -> list:
            metadata_keys = {row[0] for row in metadata if row}
            new_files = [file for file in files if file not in metadata_keys]

            return [f"s3a://{bucket_name}/{f}" for f in new_files]

        new_files = select_new_files(list_files.output, metadata, Minio.BUCKET_NAME)


    with TaskGroup(group_id="process_files") as process:
        process_file = SparkSubmitOperator(
            task_id='run_spark_job',
            application='/opt/airflow/dags/scripts/bronze_client_ingest.py',
            name='arrow-spark',
            conn_id='spark_default',
            packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.540,org.postgresql:postgresql:42.7.2',
            py_files='/opt/airflow/dags/scripts/settings.py',
            application_args=new_files
        )

wait_for_file >> [list_files, metadata] >> new_files >> process_file