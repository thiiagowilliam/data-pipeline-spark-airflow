from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.sensors.external_task import ExternalTaskSensor
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
    dag_id="dbt_gold_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dbt", "gold"],
    doc_md="""

    Este DAG executa os modelos da camada Gold do dbt usando Cosmos.
    Ele aguarda a conclusão da DAG da pipeline Silver antes de iniciar.
    """,
) as dag:
    wait_for_silver_dag = ExternalTaskSensor(
        task_id="wait_for_silver_dag",
        external_dag_id="dbt_silver_pipeline",
        external_task_id=None,
        timeout=600,
        allowed_states=["success"],
        poke_interval=15,
    )

    gold_group = DbtTaskGroup(
        group_id="dbt_gold_tasks",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["tag:gold"],
            render_mode=RenderMode.DAG,
        ),
    )

    wait_for_silver_dag >> gold_group
