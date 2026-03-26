from __future__ import annotations

import sys
import os
import json
import uuid
import logging
import argparse
from typing import Optional, List
from dataclasses import dataclass
from pydantic import BaseModel, Field as PydanticField
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

from spark_config import get_spark_session

# Configuração de Logging idêntica à sua
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
    layer: str
    table: str
    input_path: str
    execution_date: str
    run_id: str = ""
    
    @property
    def full_table_id(self) -> str:
        return f"{self.project_id}.{self.layer}.{self.table}"

class FieldContract(BaseModel):
    name: str
    type: str
    not_null: bool = False
    regex: Optional[str] = None
    unique: bool = False
    pii: bool = False  # <-- Adicionado para controle de privacidade

class Contract(BaseModel):
    dataset: str
    fields: List[FieldContract]

class BigQueryLoadEngine:
    def __init__(self, spark: SparkSession, config: BigQueryLoadConfig, contract: Contract):
        self.spark = spark
        self.config = config
        self.contract = contract
        
        base_logger = logging.getLogger("BQ_Engine")
        self.logger = RunIdAdapter(base_logger, {"run_id": self.config.run_id})
    
    def extract(self) -> DataFrame:
        self.logger.info(f"Extraindo dados da Silver: {self.config.input_path}")
        return self.spark.read.format("delta").load(self.config.input_path) \
                   .filter(F.col("dt_ingestao") == F.lit(self.config.execution_date))

    def _apply_pii_protection(self, df: DataFrame) -> DataFrame:
        """
        Identifica campos PII no contrato e aplica SHA-256.
        Isso mantém a cardinalidade (permite JOINs) mas protege o dado sensível.
        """
        pii_fields = [f.name for f in self.contract.fields if f.pii]
        
        if not pii_fields:
            return df
            
        self.logger.info(f"Protegendo campos PII: {pii_fields}")
        for field in pii_fields:
            # Aplicamos SHA-256 e convertemos para string para garantir compatibilidade no BQ
            df = df.withColumn(
                field, 
                F.sha2(F.col(field).cast("string"), 256)
            )
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        self.logger.info("Iniciando transformações: PII Protection + Metadados.")
        
        # 1. Proteção de dados sensíveis
        df = self._apply_pii_protection(df)
        
        # 2. Adição de metadados de auditoria para o BigQuery
        return df.withColumn("_bq_run_id", F.lit(self.config.run_id)) \
                 .withColumn("_bq_loaded_at", F.current_timestamp())

    def load(self, df: DataFrame):
        self.logger.info(f"Efetuando LOAD no BigQuery: {self.config.full_table_id}")
        
        # Aqui usamos o conector spark-bigquery
        df.write.format("bigquery") \
            .option("table", self.config.full_table_id) \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
            
        self.logger.info("Processo de carga concluído com sucesso!")

    def execute_pipeline(self):
        df_silver = self.extract()
        
        if df_silver.isEmpty():
            self.logger.warning(f"Sem dados na Silver para {self.config.execution_date}. Abortando.")
            return

        df_transformed = self.transform(df_silver)
        self.load(df_transformed)


def load_contract(contracts_path: str, table_name: str) -> Contract:
    file_path = os.path.join(contracts_path, f"{table_name}.json")
    with open(file_path, "r", encoding="utf-8") as f:
        return Contract(**json.load(f))

def parse_args() -> tuple[BigQueryLoadConfig, str]:
    parser = argparse.ArgumentParser(description="Pipeline Silver -> BigQuery com PII")
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--layer", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--contracts_path", required=True)
    parser.add_argument("--execution_date", required=True)
    args = parser.parse_args()
    
    run_id = str(uuid.uuid4())[:8]
    config = BigQueryLoadConfig(
        project_id=args.project_id,
        layer=args.layer,
        table=args.table,
        input_path=args.input_path,
        execution_date=args.execution_date,
        run_id=run_id
    )
    return config, args.contracts_path


if __name__ == "__main__":
    config, contracts_path = parse_args()
    
    base_main_logger = logging.getLogger("Main_BQ_Pipeline")
    main_logger = RunIdAdapter(base_main_logger, {"run_id": config.run_id})
    
    spark = get_spark_session(f"BQ_Load_{config.table}_{config.run_id}")
    
    try:
        contract = load_contract(contracts_path, config.table)
        engine = BigQueryLoadEngine(spark=spark, config=config, contract=contract)
        engine.execute_pipeline()
        
    except Exception as e:
        main_logger.critical(f"Falha catastrófica: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()