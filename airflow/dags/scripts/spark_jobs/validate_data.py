"""
Spark Job de Validação de Dados — Camada Bronze.

Valida arquivos CSV do MinIO/S3 contra contratos de dados antes da ingestão.
Projetado para uso com SparkSubmitOperator no Airflow.

Uso:
    spark-submit validate_data.py s3a://datalake/raw/clientes/file.csv ...
"""

from __future__ import annotations

import os
import sys
import logging

from spark_config import get_spark_session
from validate import DataValidator, DataValidationError

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("validate_data")


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
# Main
# ──────────────────────────────────────────────


def main() -> None:
    files_to_validate = sys.argv[1:]
    if not files_to_validate:
        log.warning("Nenhum arquivo recebido para validação. Encerrando.")
        sys.exit(0)

    log.info("═" * 60)
    log.info("  INÍCIO DA VALIDAÇÃO DE DADOS — CAMADA BRONZE")
    log.info("  Arquivos: %d", len(files_to_validate))
    log.info("═" * 60)

    spark = get_spark_session("Data Validation — Bronze Layer")
    contract_dir = os.getenv("CONTRACT_DIR", "/opt/airflow/dags/contracts")
    validator = DataValidator(contract_dir=contract_dir)

    all_valid = True
    results: list[dict] = []

    for file_path in files_to_validate:
        log.info("─" * 60)
        log.info("Processando: %s", file_path)

        try:
            dataset = detect_dataset(file_path)
        except ValueError as e:
            log.error("Erro ao detectar dataset: %s", e)
            all_valid = False
            results.append(
                {"file": file_path, "dataset": "unknown", "status": "ERRO"}
            )
            continue

        # Job group para rastreamento no Spark UI
        job_id = f"validate_{dataset}_{file_path.split('/')[-1].replace('.csv', '')}"
        description = f"Validando {dataset} — {file_path}"
        spark.sparkContext.setJobGroup(job_id, description)

        try:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            report = validator.validate(df, dataset)

            if not report.is_valid:
                all_valid = False
                results.append(
                    {
                        "file": file_path,
                        "dataset": dataset,
                        "status": "REPROVADO",
                        "errors": len(report.errors),
                    }
                )
            else:
                results.append(
                    {
                        "file": file_path,
                        "dataset": dataset,
                        "status": "APROVADO",
                    }
                )

        except DataValidationError as e:
            log.error("Validação falhou: %s", e)
            all_valid = False
            results.append(
                {"file": file_path, "dataset": dataset, "status": "REPROVADO"}
            )
        except Exception as e:
            log.error("Erro inesperado ao validar %s: %s", file_path, e)
            all_valid = False
            results.append(
                {"file": file_path, "dataset": dataset, "status": "ERRO"}
            )

    # ── Resumo Final ──
    log.info("═" * 60)
    log.info("  RESUMO FINAL DA VALIDAÇÃO")
    log.info("═" * 60)
    for r in results:
        icon = "✅" if r["status"] == "APROVADO" else "❌"
        log.info(
            "  %s %s [%s] → %s",
            icon,
            r["file"].split("/")[-1],
            r["dataset"],
            r["status"],
        )
    log.info("═" * 60)

    spark.stop()

    if not all_valid:
        log.error("Uma ou mais validações falharam. Pipeline interrompido.")
        sys.exit(1)

    log.info("Todas as validações passaram com sucesso.")
    sys.exit(0)


if __name__ == "__main__":
    main()
