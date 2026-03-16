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


def detect_dataset(file_path: str) -> str:
    path_lower = file_path.lower()
    if "clientes" in path_lower:
        return "clientes"
    if "vendas" in path_lower:
        return "vendas"
    raise ValueError(f"Dataset não identificado no path: {file_path}")


def save_to_bigquery(df, spark: SparkSession, dataset: str, project_id: str, temp_bucket: str) -> None:
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

def process_datasets(delta_paths: list[str], spark: SparkSession, execution_date: str, project_id: str, temp_bucket: str):
    for path in delta_paths:
        dataset = detect_dataset(path)
        log.info("A ler o dataset %s a partir da camada Bronze (MinIO): %s", dataset, path)
        
        read_job_id = f"read_delta_{dataset}"
        spark.sparkContext.setJobGroup(read_job_id, f"Reading Delta {dataset}")
        df = spark.read.format("delta").load(path)
        if "dt_ingestao" not in df.columns:
            log.warning("Tabela Delta antiga detectada (sem dt_ingestao). Adicionando a coluna no ar e usando todos os dados...")
            df_partition = df.withColumn("dt_ingestao", lit(execution_date))
        else:
            df_partition = df.filter(col("dt_ingestao") == execution_date)

        if df_partition.isEmpty():
            log.warning("Nenhum dado encontrado para %s na data %s. Pulando...", dataset, execution_date)
            continue
            
        save_to_bigquery(df_partition, spark, dataset, project_id, temp_bucket)


if __name__ == "__main__":
    if len(sys.argv) < 6:
        log.error("Argumentos insuficientes. Esperado: <execution_date> <project_id> <temp_bucket> <delta_paths...>")
        sys.exit(1)
    print(sys.argv)
    execution_date = sys.argv[1]
    project_id = sys.argv[2]
    temp_bucket = sys.argv[3]
    delta_paths = sys.argv[4:]     

    spark = get_spark_session("Bronze to BQ — Data Pipeline")
    
    try:
        process_datasets(delta_paths, spark, execution_date, project_id, temp_bucket)
    except Exception as e:
        log.error("Pipeline falhou: %s", e)
        spark.stop()
        sys.exit(1)

    spark.stop()
    sys.exit(0)