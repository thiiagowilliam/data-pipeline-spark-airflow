from __future__ import annotations

import sys
import logging
import json
import uuid
import argparse
from typing import List, Tuple
from functools import reduce

from pydantic import BaseModel, Field
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, lit, when, date_format, trim, row_number, to_date
from pyspark.sql.types import (
    StringType, IntegerType, BooleanType, DoubleType, DateType, TimestampType, LongType
)
from pyspark.storagelevel import StorageLevel
from delta.tables import DeltaTable 

from spark_config import get_spark_session

CONTRACTS_PATH = "/opt/airflow/dags/contracts"

SPARK_TYPE_MAP = {
    "string": StringType(),
    "integer": LongType(),
    "long": LongType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}

class FieldContract(BaseModel):
    name: str
    type: str = Field(..., alias="type")
    unique: bool = False
    regex: str | None = None
    in_set: List[str] | None = None
    pii: bool = False

    @property
    def spark_type(self):
        clean = self.type.lower().strip()
        return SPARK_TYPE_MAP.get(clean, StringType())

class DatasetContract(BaseModel):
    table: str
    description: str | None = None
    fields: List[FieldContract]

class DataInfo(BaseModel):
    project_id: str
    datalake: str
    table: str
    execution_date: str


def parse_args() -> DataInfo:
    parser = argparse.ArgumentParser(description="Bronze to Silver Ingest")
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--datalake", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--execution_date", required=True)
    parsed = parser.parse_args()
    return DataInfo(**vars(parsed))

def get_contract(table_name: str) -> DatasetContract:
    contract_path = f"{CONTRACTS_PATH}/{table_name}.json"
    try:
        with open(contract_path, "r", encoding="utf-8") as f:
            return DatasetContract(**json.load(f))
    except Exception as e:
        logging.getLogger(__name__).error(f"Falha ao carregar contrato {table_name}: {e}")
        raise

class QualityAndIngest:
    def __init__(self, spark: SparkSession, info: DataInfo, contract: DatasetContract):
        self.spark = spark
        self.info = info
        self.table = contract.table
        self.fields = contract.fields
        self.run_id = str(uuid.uuid4())
        self.repartition_col = "dt_ingest" 
        self.bronze_path = f"s3a://{self.info.datalake}/bronze/{self.table}"
        self.silver_path = f"s3a://{self.info.datalake}/silver/{self.table}"
        self.quarantine_path = f"s3a://{self.info.datalake}/quarantine/{self.table}"
        self.checkpoint = f"s3a://{self.info.datalake}/checkpoints/bronze_to_silver/{self.table}"
        base_logger = logging.getLogger("BronzeToSilver")
        self.logger = logging.LoggerAdapter(base_logger, {"run_id": self.run_id})
        self._apply_tuning()

    def _apply_pii_protection(self, df: DataFrame) -> DataFrame:
        pii_fields = [f.name for f in self.contract.fields if f.pii]
        if not pii_fields:
            return df
        
        self.logger.info(f"Protegendo campos PII (SHA-256): {pii_fields}")
        for field in pii_fields:
            if field in df.columns:
                df = df.withColumn(field, F.sha2(F.col(field).cast("string"), 256))
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        self.logger.info("Iniciando transformações: PII Protection + Metadados.")
        df = self._apply_pii_protection(df)
        return df


    def _apply_tuning(self):
        conf = self.spark.conf
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
        conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
        conf.set("spark.sql.shuffle.partitions", "200")
        conf.set("spark.sql.files.maxPartitionBytes", "512m")
        conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        conf.set("spark.sql.autoBroadcastJoinThreshold", "300m")
        conf.set("spark.sql.adaptive.optimizeSkewedJoin", "true")


    def validate(self, df: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
        for field in self.fields:
            df = df.withColumn(field.name, trim(col(field.name).cast("string")))

        conditions = []
        for field in self.fields:
            if field.type.lower() not in ["string", "date", "timestamp"]:
                invalid_type = col(field.name).isNotNull() & col(field.name).cast(field.spark_type).isNull()
                conditions.append(invalid_type)

            if field.regex:
                invalid_regex = col(field.name).isNotNull() & (~col(field.name).rlike(field.regex))
                conditions.append(invalid_regex)

            if field.in_set:
                invalid_value = col(field.name).isNotNull() & (~col(field.name).isin(field.in_set))
                conditions.append(invalid_value)

        cheap_errors = reduce(lambda x, y: x | y, conditions, lit(False)) if conditions else lit(False)

        df_validate = df.withColumn("erros", cheap_errors)
        
        unique_fields = [f for f in self.fields if f.unique]
        for field in unique_fields:
            window = Window.partitionBy(field.name).orderBy(lit(1)) 
            dup_col = f"dup_{field.name}"
            df_validate = df_validate.withColumn(
                dup_col,
                when(
                    col(field.name).isNotNull() & (row_number().over(window) > 1),
                    lit(True)
                ).otherwise(lit(False))
            )

          
            df_validate = df_validate.withColumn("erros", col("erros") | col(dup_col)).drop(dup_col)
        df_validate = self.transform(df_validate)
        df_validate.persist(StorageLevel.MEMORY_AND_DISK)
        df_erros = df_validate.filter(col("erros") == True).drop("erros")
        df_validos = df_validate.filter(col("erros") == False).drop("erros")

        return df_validos, df_erros, df_validate


    def _process_batch(self, dataframe: DataFrame, batch_id: int) -> None:
        self.logger.info(f"Iniciando micro-batch ID: {batch_id}")
        
        if dataframe.isEmpty():
            self.logger.info("Micro-batch vazio → pulando")
            return
        
        dataframe.persist(StorageLevel.MEMORY_AND_DISK)
        df_validate = None
        
        try:
            df_validos, df_erros, df_validate = self.validate(dataframe)
            if not df_erros.isEmpty():
                self.logger.info("Enviando registros inválidos para quarentena")
                (df_erros.write
                    .format("delta")
                    .mode("append")
                    .partitionBy(self.repartition_col)
                    .option("optimizeWrite", "true")
                    .option("mergeSchema", "true")
                    .save(self.quarantine_path))

            if not df_validos.isEmpty():
                casts = {f.name: col(f.name).cast(f.spark_type) for f in self.fields}
                df_validos = df_validos.withColumns(casts)
                
                unique_keys = [f.name for f in self.fields if f.unique]
                
                if unique_keys and DeltaTable.isDeltaTable(self.spark, self.silver_path):
                    self.logger.info(f"Realizando UPSERT na Silver usando as chaves: {unique_keys}")
                    delta_silver = DeltaTable.forPath(self.spark, self.silver_path)
                    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in unique_keys])
                    (delta_silver.alias("target")
                        .merge(
                            df_validos.alias("source"),
                            merge_condition
                        )
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute())
                else:
                    self.logger.info(f"Carga inicial ou sem chaves únicas. Gravando na Silver ({self.silver_path})")
                    (df_validos.write
                        .format("delta")
                        .mode("append")
                        .partitionBy(self.repartition_col)
                        .option("optimizeWrite", "true")
                        .save(self.silver_path))
        finally:
            dataframe.unpersist()
            if df_validate is not None:
                df_validate.unpersist()
            self.logger.info(f"Micro-batch {batch_id} finalizado e memória liberada")

    def run_pipeline(self):
        self.logger.info(f"Iniciando pipeline Bronze → Silver: {self.bronze_path}")

        if not DeltaTable.isDeltaTable(self.spark, self.bronze_path):
            self.logger.warning(f"A tabela Bronze em {self.bronze_path} ainda não existe.")
            return
        
        try:
            df_stream = self.spark.readStream.format("delta").load(self.bronze_path)

            query = (df_stream.writeStream
                     .foreachBatch(self._process_batch)
                     .option("checkpointLocation", self.checkpoint)
                     .trigger(availableNow=True)
                     .start())

            query.awaitTermination()
            self.logger.info("Pipeline Bronze → Silver concluído com sucesso!")

        except Exception as e:
            self.logger.error("Falha crítica no pipeline", exc_info=True)
            raise

if __name__ == "__main__":
    spark = get_spark_session("Bronze_To_Silver_Quality")
    try:
        info = parse_args()
        contract = get_contract(info.table)
        engine = QualityAndIngest(spark, info, contract)
        engine.run_pipeline()

    except Exception as e:
        logging.critical(f"Job falhou: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()