from __future__ import annotations
import sys
import logging
import json
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from spark_config import get_spark_session
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DataQuality_Auditor")

class ClientesValidator:
    def __init__(self, df: DataFrame, dataset_name: str):
        self.df = df
        self.ge_df = SparkDFDataset(df)
        self.dataset_name = dataset_name
        self.contract_path = f"/opt/airflow/contracts/{dataset_name}.json"

    def _check_schema(self):
        if os.path.exists(self.contract_path):
            with open(self.contract_path, "r") as f:
                contract = json.load(f)
            columns = [field["name"] for field in contract.get("fields", [])]
            for col_name in columns:
                self.ge_df.expect_column_to_exist(col_name)
            logger.info(f"Schema validado contra contrato: {self.contract_path}")

    def execute_audit(self):
        self._check_schema()
        self.ge_df.expect_column_values_to_not_be_null("id")
        self.ge_df.expect_column_values_to_be_unique("id")
        self.ge_df.expect_column_values_to_not_be_null("email")
        
        self.ge_df.expect_column_values_to_be_in_set(
            "status", ["Ativo", "Inativo", "Pendente"]
        )
        
        self.ge_df.expect_column_values_to_match_regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")

        return self.ge_df.validate()

def normalize_data(df: DataFrame) -> DataFrame:
    if "status" in df.columns:
        df = df.withColumn("status", F.initcap(F.trim(F.col("status"))))
    if "email" in df.columns:
        df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    return df

if __name__ == "__main__":
    if len(sys.argv) < 4:
        logger.error("Argumentos insuficientes! Uso: <dataset> <path> <date>")
        sys.exit(1)

    dataset_name = sys.argv[1]
    delta_path   = sys.argv[2]
    exec_date    = sys.argv[3]

    spark = get_spark_session(f"Audit_{dataset_name}")

    try:
        raw_df = spark.read.format("delta").load(delta_path)
        daily_df = raw_df.filter(F.col("dt_ingestao") == exec_date)

        if daily_df.isEmpty():
            logger.warning(f"Sem novos dados para validar em {exec_date}.")
            sys.exit(0)

        clean_df = normalize_data(daily_df)
        validator = ClientesValidator(clean_df, dataset_name)
        results = validator.execute_audit()

        if not results["success"]:
            logger.error(f"FALHA NO CONTRATO DE DADOS: {dataset_name}")
            quarantine_path = f"s3a://datalake/quarantine/{dataset_name}/dt={exec_date}"
            clean_df.withColumn("audit_failure_details", F.lit(str(results["results"]))) \
                    .write.format("parquet").mode("overwrite").save(quarantine_path)
            
            logger.info(f"Dados suspeitos movidos para quarentena: {quarantine_path}")
            sys.exit(1)

        logger.info(f"✅ {dataset_name} aprovado e pronto para a Silver!")
        
    except Exception as e:
        logger.error(f"Erro fatal: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()