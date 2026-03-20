from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from cosmos.providers.dbt.task_group import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import RenderMode

profile_config = ProfileConfig(
    profile_name="bigquery",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dags/dbt/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dags/dbt/",
)


with DAG(
    dag_id="dbt_silver_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dbt", "silver"],
    doc_md="""
    Este DAG executa os modelos da camada Silver do dbt usando Cosmos.
    Cada modelo e teste é representado como uma tarefa individual no Airflow.
    """,
) as dag:
    silver_group = DbtTaskGroup(
        group_id="dbt_silver_tasks",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["tag:silver"],
            render_mode=RenderMode.DAG,
        ),
    )
