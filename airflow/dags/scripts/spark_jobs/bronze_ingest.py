"""
Spark Job de Ingestão Bronze — Data Pipeline.

Lê arquivos CSV do MinIO/S3, valida contra contratos de dados,
converte para Delta Lake e carrega no PostgreSQL.

Uso:
    spark-submit bronze_ingest.py s3a://datalake/raw/clientes/file.csv ...
"""

from __future__ import annotations

import os
import sys
import logging

from pyspark.sql import SparkSession

from spark_config import get_spark_session
from validate import validate_bronze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bronze_ingest")


# ──────────────────────────────────────────────
# Dataset Detection
# ──────────────────────────────────────────────


def detect_dataset(file_path: str) -> str:
    """Identifica o dataset com base no path do arquivo."""
    path_lower = file_path.lower()
    if "clientes" in path_lower:
        return "clientes"
    if "vendas" in path_lower:
        return "vendas"
    raise ValueError(f"Dataset não identificado no path: {file_path}")


# ──────────────────────────────────────────────
# Delta Lake Write
# ──────────────────────────────────────────────


def save_delta(
    df, spark: SparkSession, delta_path: str, dataset: str, file_path: str
) -> None:
    """Salva DataFrame em formato Delta Lake com job group no Spark UI."""
    file_id = file_path.split("/")[-1].replace(".csv", "")
    job_id = f"write_{dataset}_{file_id}"
    description = f"Writing {dataset} from {file_path} → {delta_path}"

    spark.sparkContext.setJobGroup(job_id, description)
    df.write.format("delta").mode("overwrite").save(delta_path)
    log.info("Delta write: %s → %s", file_path, delta_path)


# ──────────────────────────────────────────────
# PostgreSQL Load
# ──────────────────────────────────────────────


def load_to_postgres(spark: SparkSession, delta_files: list[tuple]) -> None:
    """Carrega dados do Delta Lake para tabelas staging no PostgreSQL."""
    jdbc_url = "jdbc:postgresql://{}:{}/{}".format(
        os.getenv("DB_HOST", "postgres"),
        os.getenv("DB_PORT", "5432"),
        os.getenv("DB_NAME", "airflow"),
    )
    db_user = os.getenv("DB_USER", "airflow")
    db_password = os.getenv("DB_PASSWORD", "airflow")

    for delta_path, dataset in delta_files:
        job_id = f"insert_{dataset}_{delta_path.split('/')[-1]}"
        description = f"Inserting Delta {delta_path} → {dataset}_staging"
        spark.sparkContext.setJobGroup(job_id, description)

        df = spark.read.format("delta").load(delta_path)
        df = df.dropDuplicates(["id"])

        (
            df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"{dataset}_staging")
            .option("user", db_user)
            .option("password", db_password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        log.info("Postgres load: %s → %s_staging", delta_path, dataset)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────


def main() -> None:
    files_to_process = sys.argv[1:]
    if not files_to_process:
        log.warning("Nenhum arquivo recebido do Airflow. Encerrando.")
        sys.exit(0)

    log.info("═" * 60)
    log.info("  INÍCIO DA INGESTÃO BRONZE")
    log.info("  Arquivos: %d", len(files_to_process))
    log.info("═" * 60)

    spark = get_spark_session("Bronze Ingest — Data Pipeline")
    delta_files: list[tuple] = []

    for file_path in files_to_process:
        dataset = detect_dataset(file_path)
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Validação
        job_id = f"validate_{dataset}_{file_path.split('/')[-1]}"
        spark.sparkContext.setJobGroup(job_id, f"Validating {dataset}")
        try:
            validate_bronze(df, dataset)
            log.info("Validação aprovada: %s", file_path)
        except Exception as e:
            log.error("Validação falhou para %s: %s", file_path, e)
            sys.exit(1)

        # Delta write
        file_id = file_path.split("/")[-1].replace(".csv", "")
        delta_path = f"s3a://datalake/bronze/{dataset}/{file_id}"
        save_delta(df, spark, delta_path, dataset, file_path)
        delta_files.append((delta_path, dataset))

    # PostgreSQL load
    load_to_postgres(spark, delta_files)

    log.info("═" * 60)
    log.info("  INGESTÃO BRONZE CONCLUÍDA COM SUCESSO")
    log.info("═" * 60)
    spark.stop()


if __name__ == "__main__":
    main()