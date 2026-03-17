from __future__ import annotations

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit

from spark_config import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_bigquery")

def save_to_bigquery(df, spark: SparkSession, dataset: str, project_id: str) -> None:
    job_id = f"write_bq_{dataset}"
    spark.sparkContext.setJobGroup(job_id, f"Writing {dataset} to BigQuery")
    table_id = f"{project_id}.{dataset}.bronze" 
    coluna_cluster = "id" if dataset == "clientes" else "cliente_id"
    
    log.info("A gravar no BigQuery (MODO DIRETO) -> Tabela: %s", table_id)
    
    df_bq = df.withColumn("dt_ingestao", to_date(col("dt_ingestao")))
    
    df_bq.write.format("bigquery") \
        .option("table", table_id) \
        .option("project", project_id) \
        .option("parentProject", project_id) \
        .option("writeMethod", "direct") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("partitionField", "dt_ingestao") \
        .option("partitionType", "DAY") \
        .option("clusteredFields", coluna_cluster) \
        .mode("append") \
        .save()
        
    log.info("✅ Sucesso! Camada Bronze de %s atualizada no BigQuery.", dataset)

def process_datasets(delta_path: str, dataset: str, spark: SparkSession, execution_date: str, project_id: str):
    spark.sparkContext.setJobGroup(f"read_delta_{dataset}", f"Reading Delta {dataset}")
    df = spark.read.format("delta").load(delta_path)
    
    if "dt_ingestao" not in df.columns:
        log.warning("Tabela Delta antiga detectada (sem dt_ingestao). Adicionando a coluna no ar e usando todos os dados...")
        df_partition = df.withColumn("dt_ingestao", lit(execution_date))
    else:
        df_partition = df.filter(col("dt_ingestao") == execution_date)

    if df_partition.isEmpty():
        log.warning("Nenhum dado encontrado para %s na data %s. Pulando...", dataset, execution_date)
        return
        
    save_to_bigquery(df_partition, spark, dataset, project_id)

if __name__ == "__main__":
    log.info(f"Argumentos recebidos: {sys.argv}")
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