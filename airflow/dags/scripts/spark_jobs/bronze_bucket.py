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


def detect_dataset(file_path: str) -> str:
    path_lower = file_path.lower()
    if "clientes" in path_lower:
        return "clientes"
    if "vendas" in path_lower:
        return "vendas"
    raise ValueError(f"Dataset não identificado no path: {file_path}")


def save_delta(df, spark: SparkSession, delta_path: str, dataset: str, execution_date: str) -> None:
    job_id = f"write_{dataset}"
    spark.sparkContext.setJobGroup(job_id, f"Writing {dataset} (Partitioned)")

    df_with_date = df.withColumn("dt_ingestao", lit(execution_date))

    log.info("Gravando %s via Delta Lake (partition dt_ingestao) → %s", dataset, delta_path)

    df_with_date.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("dt_ingestao") \
        .save(delta_path)

    # Z-ORDER (melhora performance de leitura)
    coluna_zorder = "id" if dataset == "clientes" else "cliente_id"
    log.info("Executando OPTIMIZE + Z-ORDER BY %s em %s...", coluna_zorder, dataset)
    spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY ({coluna_zorder})")

    log.info("✅ Sucesso! Dataset %s carregado no Delta.", dataset)


def process_and_save_delta(files: list[str], spark: SparkSession, prefix_delta_path: str, execution_date: str):
    datasets = {}
    for file in files:
        ds = detect_dataset(file)
        datasets.setdefault(ds, []).append(file)

    schema_clientes = "id BIGINT, nome STRING, email STRING, telefone STRING, cidade STRING, estado STRING, data_cadastro STRING, status STRING"
    schema_vendas = "id BIGINT, cliente_id BIGINT, produto_id BIGINT, data_venda STRING, valor_total DOUBLE, quantidade INT, metodo_pagto STRING"

    for dataset, file_paths in datasets.items():
        log.info("Processando dataset %s → arquivos: %s", dataset, file_paths)

        schema_correto = schema_clientes if dataset == "clientes" else schema_vendas

        df = spark.read.option("header", True).schema(schema_correto).csv(file_paths)

        if df.isEmpty():
            log.warning("Nenhum dado encontrado para %s. Pulando...", dataset)
            continue

        delta_path = f"{prefix_delta_path}{dataset}"
        save_delta(df, spark, delta_path, dataset, execution_date)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        log.error("Argumentos insuficientes. Esperado: <execution_date> <file_paths...>")
        sys.exit(1)

    execution_date = sys.argv[1]
    files = sys.argv[2:]

    spark = get_spark_session("Bronze Ingest — Delta Lake")
    prefix_delta_path = "s3a://datalake/bronze_layer/"   # ← pode deixar hardcoded mesmo

    try:
        process_and_save_delta(files, spark, prefix_delta_path, execution_date)
    except Exception as e:
        log.error("Pipeline falhou: %s", e)
        spark.stop()
        sys.exit(1)

    spark.stop()
    sys.exit(0)