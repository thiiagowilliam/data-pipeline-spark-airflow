from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
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
    dag_id="dbt_gold_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "gold", "sensor"],
    doc_md="Monitora a silver aguardando o particionamento do dia.",
) as dag:
    
    wait_for_silver_data = SqlSensor(
        task_id="wait_for_silver_tables_data",
        conn_id="bigquery_default",
        sql="""
        SELECT
          CASE
            WHEN (SELECT COUNT(*) FROM `{{ var.value.gcp_project }}`.clientes.silver WHERE data_cadastro = '{{ ds }}') > 0
             AND (SELECT COUNT(*) FROM `{{ var.value.gcp_project }}`.vendas.silver WHERE data_venda = '{{ ds }}') > 0
            THEN 1
            ELSE 0
          END
        """,
        success=lambda x: x[0][0] == 1 if x else False,
        poke_interval=60 * 5,
        timeout=60 * 60 * 2,
        mode="poke",
    )

    gold_group = DbtTaskGroup(
        group_id="dbt_gold_tasks",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["tag:gold"],
        ),
    )

    wait_for_silver_data >> gold_group