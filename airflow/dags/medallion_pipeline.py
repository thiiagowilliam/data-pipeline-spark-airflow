# -*- coding: utf-8 -*-
"""
### Medallion Sales Pipeline
A DAG that orchestrates the ingestion and processing of sales data through a Medallion architecture
using the Spark on Kubernetes Operator.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# Define the paths to the SparkApplication YAML files within the repository.
# The Airflow environment is assumed to have a Git-Sync mechanism that clones the repo.
BRONZE_APP_FILE = "infra/kubernetes/spark-applications/bronze.yaml"
SILVER_APP_FILE = "infra/kubernetes/spark-applications/silver.yaml"
GOLD_APP_FILE = "infra/kubernetes/spark-applications/gold.yaml"

# Define the Kubernetes namespace where the Spark jobs will run.
SPARK_JOBS_NAMESPACE = "spark-jobs"


@dag(
    dag_id="medallion_sales_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "DataEngineeringTeam",
        "retries": 2,
    },
    tags=["pyspark", "kubernetes", "spark-operator", "medallion"],
)
def medallion_sales_pipeline():
    """
    ### Medallion Sales Pipeline
    This DAG runs a series of Spark jobs to process sales data, following a Medallion architecture.
    - **Bronze:** Ingests raw data from a source system.
    - **Silver:** Cleans, deduplicates, and validates the data.
    - **Gold:** Aggregates the data for business intelligence and reporting.
    """

    ingest_bronze = SparkKubernetesOperator(
        task_id="ingest_bronze",
        application_file=BRONZE_APP_FILE,
        namespace=SPARK_JOBS_NAMESPACE,
        kubernetes_conn_id="kubernetes_default",  # Assumes in-cluster config
        do_xcom_push=True,
    )

    @task(task_id="run_data_quality_checks")
    def validate_silver_data(bronze_output: dict) -> str:
        """
        #### Run Data Quality Checks
        A placeholder task for running data quality checks, for example, using Great Expectations.
        This task would typically be triggered after the Silver layer is populated and before
        the Gold layer is generated. For now, it simulates a check based on the XCom output
        from the Spark job.
        """
        print(f"Received XCom output from Spark job: {bronze_output}")
        # In a real scenario, this task would use the data location from the output
        # to run a Great Expectations checkpoint.
        print("Simulating data quality checks on Silver data... all checks passed!")
        return "validation_complete"


    transform_silver = SparkKubernetesOperator(
        task_id="transform_silver",
        application_file=SILVER_APP_FILE,
        namespace=SPARK_JOBS_NAMESPACE,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    aggregate_gold = SparkKubernetesOperator(
        task_id="aggregate_gold",
        application_file=GOLD_APP_FILE,
        namespace=SPARK_JOBS_NAMESPACE,
        kubernetes_conn_id="kubernetes_default",
    )

    @task(task_id="notify_pipeline_completion")
    def notify_completion(gold_output: dict | None = None):
        """
        #### Notify Pipeline Completion
        Sends a notification (e.g., email or Slack message) that the pipeline has completed successfully.
        """
        print(f"Pipeline finished successfully. Gold data is ready.")
        if gold_output:
            print(f"Gold task output: {gold_output}")


    # Define task dependencies
    validation_result = validate_silver_data(ingest_bronze.output)
    ingest_bronze >> transform_silver
    transform_silver >> validation_result
    validation_result >> aggregate_gold
    aggregate_gold >> notify_completion()


medallion_sales_pipeline()