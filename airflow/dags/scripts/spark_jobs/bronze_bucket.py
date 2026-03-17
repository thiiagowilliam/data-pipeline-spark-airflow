from __future__ import annotations

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

from spark_config import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_delta")

def save_delta(df, spark: SparkSession, delta_path: str, dataset: str, execution_date: str) -> None:
    job_id = f"write_{dataset}"
    spark.sparkContext.setJobGroup(job_id, f"Writing {dataset} (Partitioned)")
    df_with_date = df.withColumn("dt_ingestao", lit(execution_date))
    log.info("Gravando %s via Delta Lake (partition dt_ingestao) → %s", dataset, delta_path)

    df_with_date.repartition(50).write.format("delta") \
        .mode("overwrite") \
        .option("inferSchema", True) \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("dt_ingestao") \
        .option("writeMethod", "direct") \
        .save(delta_path)
    
    spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY (id)")
    log.info("✅ Sucesso! Dataset %s carregado no Delta.", dataset)


def process_and_save_delta(raw_path: str, spark: SparkSession, dataset: str, delta_path: str, execution_date: str):
    log.info("Processando dataset %s → lendo arquivos de: %s", dataset, raw_path)
    df = spark.read.option("header", True).csv(raw_path)
    if df.isEmpty():
        log.warning("Nenhum dado encontrado para %s. Pulando...", dataset)
        return 
    
    save_delta(df, spark, delta_path, dataset, execution_date)


if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) < 5:
        log.error("Argumentos insuficientes.")
        sys.exit(1)

    dataset = sys.argv[1]
    raw_path_csv = f"{sys.argv[2]}/*.csv"
    delta_path = sys.argv[3]
    execution_date = sys.argv[4]

    spark = get_spark_session("Bronze Ingest — Delta Lake")

    try:
        process_and_save_delta(raw_path_csv, spark, dataset, delta_path, execution_date)
    except Exception as e:
        log.error("Pipeline falhou: %s", e)
        spark.stop()
        sys.exit(1)

    spark.stop()