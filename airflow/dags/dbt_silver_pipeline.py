from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig

profile_config = ProfileConfig(
    profile_name="bigquery",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dbt/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dbt",
)

with DAG(
    dag_id="dbt_silver_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "silver", "sensor"],
) as dag:

    wait_for_bronze_data = BigQueryCheckOperator(
        task_id="wait_for_bronze_clientes_data",
        sql="""
        SELECT EXISTS (
                SELECT 1
                FROM `{{ var.value.gcp_project }}.clientes.bronze`
                WHERE data_cadastro = DATE_SUB('{{ ds }}', INTERVAL 1 DAY)
            )
        """,
        use_legacy_sql=False,
        location="US",
        gcp_conn_id="bigquery_default",
    )

    silver_group = DbtTaskGroup(
        group_id="dbt_silver_tasks",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["tag:silver"],
        ),
    )

    wait_for_bronze_data >> silver_group