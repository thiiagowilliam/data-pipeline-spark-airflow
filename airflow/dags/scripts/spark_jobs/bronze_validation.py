from __future__ import annotations
import sys
import logging
import json
import os
import argparse
import uuid
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pydantic import BaseModel, ValidationError

from spark_config import get_spark_session
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

class RunIdAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"[RunID: {self.extra.get('run_id', 'N/A')}] {msg}", kwargs

class FieldContract(BaseModel):
    name: str
    type: str
    not_null: bool = False
    unique: bool = False
    regex: Optional[str] = None
    in_set: Optional[List[str]] = None

class DatasetContract(BaseModel):
    dataset: str
    description: Optional[str] = None
    fields: List[FieldContract]

class NativeDataQualityEngine:    
    def __init__(self, df: DataFrame, contract: DatasetContract, run_id: str):
        self.df = df
        self.contract = contract
        self.run_id = run_id
        base_logger = logging.getLogger("DQ_NativeEngine")
        self.logger = RunIdAdapter(base_logger, {"run_id": self.run_id})

    def run_audit(self) -> Tuple[DataFrame, DataFrame]:
        error_conditions = []
        df_eval = self.df
        
        self.logger.info("Traduzindo regras do contrato para expressões PySpark...")

        for field in self.contract.fields:
            col_name = field.name
        
            if field.not_null:
                cond = F.when(F.col(col_name).isNull(), F.lit(f"Erro: '{col_name}' é nulo"))
                error_conditions.append(cond)
                
            if field.in_set:
                cond = F.when(
                    F.col(col_name).isNotNull() & ~F.col(col_name).isin(field.in_set), 
                    F.lit(f"Erro: '{col_name}' fora do domínio permitido")
                )
                error_conditions.append(cond)
                
            if field.regex:
                cond = F.when(
                    F.col(col_name).isNotNull() & ~F.col(col_name).rlike(field.regex), 
                    F.lit(f"Erro: '{col_name}' falhou no padrão Regex")
                )
                error_conditions.append(cond)

            if field.unique:
                w = Window.partitionBy(col_name)
                df_eval = df_eval.withColumn(f"__cnt_{col_name}", F.count(col_name).over(w))
                cond = F.when(
                    F.col(f"__cnt_{col_name}") > 1, 
                    F.lit(f"Erro: '{col_name}' possui valores duplicados")
                )
                error_conditions.append(cond)

        if not error_conditions:
            return self.df, self.df.limit(0)
        
        df_eval = df_eval.withColumn("dq_errors_raw", F.array(*error_conditions))
        df_eval = df_eval.withColumn("dq_errors", F.expr("filter(dq_errors_raw, x -> x is not null)"))
        df_eval = df_eval.drop("dq_errors_raw")

        for field in self.contract.fields:
            if field.unique:
                df_eval = df_eval.drop(f"__cnt_{field.name}")

        self.logger.info("Separando registros válidos da quarentena...")
        df_valid = df_eval.filter(F.size("dq_errors") == 0).drop("dq_errors")
        df_quarantine = df_eval.filter(F.size("dq_errors") > 0)
        
        return df_valid, df_quarantine


class PipelineIOHandler:
    @staticmethod
    def load_contract(dataset_name: str) -> DatasetContract:
        contract_path = f"/opt/airflow/dags/contracts/{dataset_name}.json"
        if not os.path.exists(contract_path):
            raise FileNotFoundError(f"Contrato inexistente: {contract_path}")
        with open(contract_path, "r", encoding="utf-8") as f:
            return DatasetContract(**json.load(f))

    @staticmethod
    def normalize_data(df: DataFrame) -> DataFrame:
        columns = df.columns
        if "status" in columns:
            df = df.withColumn("status", F.initcap(F.trim(F.col("status"))))
        if "email" in columns:
            df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
        return df

    @staticmethod
    def save_quarantine(df_quarantine: DataFrame, dataset: str, dt_ingestao: str, run_id: str):
        """Salva apenas os dados ruins, que já possuem a coluna 'dq_errors'."""
        base_path = f"s3a://datalake/quarantine/{dataset}/dt={dt_ingestao}"
        df_quarantine.withColumn("run_id", F.lit(run_id)) \
                     .write.format("parquet").mode("append").save(base_path)
        return base_path
        
    @staticmethod
    def save_layer(df_valid: DataFrame, dataset: str, dt_ingestao: str, layer: str):
        """Salva os dados limpos e aprovados na próxima camada (Silver)."""
        base_path = f"s3a://datalake/{layer}/{dataset}/dt={dt_ingestao}"
        df_valid.write.format("delta").mode("overwrite").save(base_path)
        return base_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--delta_path", required=True)
    parser.add_argument("--execution_date", required=True)
    args = parser.parse_args()
    
    run_id = str(uuid.uuid4())[:8]
    base_logger = logging.getLogger("Main_Pipeline")
    logger = RunIdAdapter(base_logger, {"run_id": run_id})
    
    spark = get_spark_session(f"DQ_{args.dataset}_{run_id}")

    try:
        logger.info(f"Iniciando Job de Qualidade. Dataset: {args.dataset}")
        
        contract = PipelineIOHandler.load_contract(args.dataset)
        raw_df = spark.read.format("delta").load(args.delta_path)
        daily_df = raw_df.filter(F.col("dt_ingestao") == F.lit(args.execution_date))
        
        if daily_df.isEmpty():
            logger.warning("Nenhum dado encontrado para a data especificada.")
            sys.exit(0)

        clean_df = PipelineIOHandler.normalize_data(daily_df)
        clean_df.cache()
        
        engine = NativeDataQualityEngine(clean_df, contract, run_id)
        df_valid, df_quarantine = engine.run_audit()
        total_valid = df_valid.count()
        total_quarantine = df_quarantine.count()

        logger.info(f"Auditoria concluída. Válidos: {total_valid} | Quarentena: {total_quarantine}")

        if total_quarantine > 0:
            logger.warning(f"Direcionando {total_quarantine} registros para a Quarentena...")
            PipelineIOHandler.save_quarantine(df_quarantine, args.dataset, args.execution_date, run_id)

        if total_valid > 0:
            logger.info("Promovendo dados válidos para a camada Silver...")
            PipelineIOHandler.save_layer(df_valid, args.dataset, args.execution_date, "silver")
            logger.info("✅ Lote processado com sucesso. Pipeline segue seu fluxo normal.")
        else:
            logger.warning("❌ Todos os registros falharam na qualidade e foram para a quarentena.")

        sys.exit(0)

    except Exception as e:
        logger.error(f"Erro fatal não tratado: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if 'clean_df' in locals():
            clean_df.unpersist()
        spark.stop()