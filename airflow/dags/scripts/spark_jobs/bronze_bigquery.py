from __future__ import annotations

import sys
import time
import logging
import argparse
import uuid
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from spark_config import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)

class RunIdAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"[RunID: {self.extra.get('run_id', 'N/A')}] {msg}", kwargs

@dataclass
class BigQueryLoadConfig:
    project_id: str
    dataset: str
    delta_path: str
    execution_date: str
    run_id: str = ""

    @property
    def table_id(self) -> str:
        return f"{self.project_id}.bronze.{self.dataset}"

class BigQueryLoadEngine:
    def __init__(self, spark: SparkSession, config: BigQueryLoadConfig):
        self.spark = spark
        self.config = config
        
        base_logger = logging.getLogger(f"BQ_Load_{config.dataset}")
        self.logger = RunIdAdapter(base_logger, {"run_id": self.config.run_id})

    def extract(self) -> Optional[DataFrame]:
        self.spark.sparkContext.setJobGroup(f"read_delta_{self.config.dataset}", "Reading Delta to BQ")
        self.logger.info(f"Lendo dados Delta de: {self.config.delta_path}")
        
        try:
            df = self.spark.read.format("delta").load(self.config.delta_path)
            df_filter = df.filter(F.col("dt_ingestao") == F.lit(self.config.execution_date))

            if df_filter.isEmpty():
                self.logger.warning(f"Nenhum dado encontrado para a data {self.config.execution_date}. Abortando carga.")
                return None
                
            return df_filter
        except Exception as e:
            self.logger.error("Erro ao ler dados do Delta Lake.", exc_info=True)
            raise

    def transform(self, df: DataFrame) -> DataFrame:
        import json
        
        self.logger.info("Aplicando casting de tipos baseado no contrato JSON...")
        contract_path = f"/opt/airflow/dags/contracts/{self.config.dataset}.json"
        
        try:
            with open(contract_path, "r", encoding="utf-8") as f:
                contract = json.load(f)

            type_mapping = {
                "integer": "int",
                "double": "double",
                "float": "float",
                "string": "string",
                "boolean": "boolean",
                "date": "date"
            }
            
            df_casted = df
            for field in contract.get("fields", []):
                col_name = field["name"]
                if col_name in df_casted.columns:
                    spark_type = type_mapping.get(field["type"].lower(), "string")
                    df_casted = df_casted.withColumn(col_name, F.col(col_name).cast(spark_type))
                    
        except FileNotFoundError:
            self.logger.warning(f"Contrato não encontrado em {contract_path}. Tentando enviar sem casting.")
            df_casted = df

        self.logger.info("Adicionando metadados de linhagem BQ...")
        return (
            df_casted.withColumn("dt_ingestao", F.to_date(F.col("dt_ingestao")))
                     .withColumn("_bq_run_id", F.lit(self.config.run_id))
                     .withColumn("_bq_loaded_at", F.current_timestamp())
        )

    def load(self, df: DataFrame) -> None:
        self.logger.info(f"Iniciando gravação no BigQuery -> Tabela: {self.config.table_id}")
        start_time = time.time()
        
        try:
            
            df.write \
                .format("bigquery") \
                .option("table", self.config.table_id) \
                .option("parentProject", self.config.project_id) \
                .option("writeMethod", "direct") \
                .option("allowFieldAddition", "true") \
                .mode("append") \
                .save()
                
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ SUCESSO: Camada Bronze atualizada no BigQuery em {elapsed_time:.2f} segundos.")
            
        except Exception as e:
            self.logger.error("Falha ao salvar dados no BigQuery.", exc_info=True)
            raise

    def execute_pipeline(self) -> None:
        df_delta = self.extract()
        
        if df_delta is not None:
            df_bq = self.transform(df_delta)
            self.load(df_bq)
        
        self.logger.info("Pipeline de carga finalizada com sucesso.")

def parse_args() -> BigQueryLoadConfig:
    parser = argparse.ArgumentParser(description="Pipeline SOTA Bronze to BQ via PySpark")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--dataset", required=True, help="Nome do dataset (ex: vendas)")
    parser.add_argument("--delta_path", required=True, help="Caminho raiz da tabela Delta")
    parser.add_argument("--execution_date", required=True, help="Data da ingestão (YYYY-MM-DD)")
    
    args = parser.parse_args()
    run_id = str(uuid.uuid4())[:8]
    
    return BigQueryLoadConfig(
        project_id=args.project_id,
        dataset=args.dataset,
        delta_path=args.delta_path,
        execution_date=args.execution_date,
        run_id=run_id
    )

if __name__ == "__main__":
    config = parse_args()
    base_main_logger = logging.getLogger("Main_BQ_Pipeline")
    main_logger = RunIdAdapter(base_main_logger, {"run_id": config.run_id})
    main_logger.info(f"Iniciando Job Delta -> BigQuery. Dataset: {config.dataset} | Partição: {config.execution_date}")
    spark = get_spark_session(f"BQ_Load_{config.dataset}_{config.run_id}")
    try:
        engine = BigQueryLoadEngine(spark=spark, config=config)
        engine.execute_pipeline()
    except Exception as e:
        main_logger.critical(f"A PIPELINE FALHOU: O processo foi interrompido. Erro: {e}", exc_info=True)
        sys.exit(1)
    finally:
        main_logger.info("Encerrando a Spark Session.")
        spark.stop()