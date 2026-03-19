from __future__ import annotations

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from spark_config import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_bigquery")

def save_to_bigquery_sandbox(df, dataset: str, project_id: str) -> None:
    table_id = f"{project_id}.{dataset}.bronze" 
    log.info("A gravar no BigQuery (MODO SANDBOX) -> Tabela: %s", table_id)
    df_bq = df.withColumn("dt_ingestao", to_date(col("dt_ingestao")))
    log.info("Convertendo dados de Spark para Pandas...")
    df_pandas = df_bq.toPandas()
    key_path = "/opt/airflow/dags/credentials/gcp_key.json"
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="dt_ingestao"
        )
    )
    
    log.info("Iniciando upload para o BigQuery...")
    job = client.load_table_from_dataframe(df_pandas, table_id, job_config=job_config)
    job.result()
    log.info("✅ Sucesso! Camada Bronze de %s atualizada no BigQuery.", dataset)

def process_datasets(delta_path: str, dataset: str, spark: SparkSession, execution_date: str, project_id: str):
    spark.sparkContext.setJobGroup(f"read_delta_{dataset}", f"Reading Delta {dataset}")
    df = spark.read.format("delta").load(delta_path)
    if "dt_ingestao" not in df.columns:
        df_partition = df.withColumn("dt_ingestao", lit(execution_date))
    else:
        df_partition = df.filter(col("dt_ingestao") == execution_date)

    if df_partition.isEmpty():
        log.warning("Nenhum dado encontrado. Pulando...")
        return
        
    save_to_bigquery_sandbox(df_partition, dataset, project_id)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        log.error("Argumentos insuficientes.")
        sys.exit(1)

    dataset = sys.argv[1]
    project_id = sys.argv[2]
    delta_path = sys.argv[3]
    execution_date = sys.argv[4]
    spark = get_spark_session("Bronze to BQ — Data Pipeline")
    
    try:
        process_datasets(delta_path, dataset, spark, execution_date, project_id)
    except Exception as e:
        log.error("Pipeline falhou: %s", e)
        spark.stop()
        sys.exit(1)

    spark.stop()
    sys.exit(0)