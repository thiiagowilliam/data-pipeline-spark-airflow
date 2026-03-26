from __future__ import annotations
import sys
import logging
import json
import uuid
from typing import List, Tuple
import argparse

from pydantic import BaseModel, Field, ValidationError
from pyspark.sql import SparkSession, DataFrame, Window
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, DoubleType, DateType, TimestampType, LongType
)

from spark_config import get_spark_session

CONTRACTS_PATH = "/opt/airflow/dags/contracts"

SPARK_TYPE_MAP = {
    "stringtype": StringType(),
    "integertype": IntegerType(),
    "longtype": LongType(),
    "doubletype": DoubleType(),
    "booleantype": BooleanType(),
    "datetype": DateType(),
    "timestamptype": TimestampType(),
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
    parser = argparse.ArgumentParser(description="Bronze Ingest — CSV → Delta Lake")
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--datalake", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--execution_date", required=True)
    parsed = parser.parse_args()
    return DataInfo(
        project_id=parsed.project_id,
        datalake=parsed.datalake,
        table=parsed.table,
        execution_date=parsed.execution_date
    )

def get_contract(table_name: str) -> DatasetContract:
    contract_path = f"{CONTRACTS_PATH}/{table_name}.json"
    try:
        with open(contract_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        contract = DatasetContract(**json_data)
        logging.info(f"Contrato {table_name} carregado com sucesso")
        return contract
    except FileNotFoundError:
        logging.error(f"Contrato {table_name}.json não encontrado em {CONTRACTS_PATH}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao parsear JSON do contrato {table_name}: {e}")
        raise
    except ValidationError as e:
        logging.error(f"Contrato inválido {table_name}: {e}")
        raise


class QualityAndIngest:
    def __init__(self, spark: SparkSession, info: DataInfo, contract: DatasetContract):
        self.spark = spark
        self.project_id = info.project_id
        self.datalake = info.datalake
        self.table = contract.table
        self.fields = contract.fields
        self.execution_date = info.execution_date
        self.run_id = str(uuid.uuid4())
        
        base_logger = logging.getLogger(__name__)
        base_logger.setLevel(logging.INFO)
        self.logger = logging.LoggerAdapter(base_logger, {"run_id": self.run_id})

        self.bronze_path = f"s3a://{self.datalake}/bronze/{self.table}"
        self.silver_path = f"s3a://{self.datalake}/silver/{self.table}"
        self.quarantine_path = f"s3a://{self.datalake}/quarantine/{self.table}"

    def get_dataframe(self) -> DataFrame:
        try:
            return self.spark.read.format("delta").load(self.bronze_path) \
                .filter(
                    (F.col("dt_ingest") == self.execution_date) & 
                    (F.col("silver_processed") == False)
                )
        except Exception as error:
            self.logger.error(f"Erro ao ler tabela Bronze: {error}", exc_info=True)
            raise error

    def validate_data(self, dataframe: DataFrame) -> Tuple[DataFrame, DataFrame]:
        df = dataframe.withColumn("erros", F.lit(False))

        for field in self.fields:
            invalid_type = F.col(field.name).isNotNull() & \
                           F.col(field.name).cast(field.spark_type).isNull()
            
            unique_value = F.lit(False)
            if field.unique:
                window_spec = Window.partitionBy(field.name)
                unique_value = F.col(field.name).isNotNull() & (F.count("*").over(window_spec) > 1)


            invalid_value = F.lit(False)
            if field.in_set:
                invalid_value = F.col(field.name).isNotNull() & \
                                (~F.col(field.name).isin(field.in_set))
                
            invalid_regex = F.lit(False)
            if field.regex:
                invalid_regex = F.col(field.name).isNotNull() & \
                                (~F.col(field.name).rlike(field.regex))

            df = df.withColumn("erros", F.col("erros") | invalid_type | unique_value | invalid_value | invalid_regex)
            
        df_erros = df.filter(F.col("erros") == True).drop("erros")
        df_validos = df.filter(F.col("erros") == False).drop("erros")
        
        return df_validos, df_erros
    
    def write_delta(self, df: DataFrame, path: str):
        try:
            df.write.format("delta") \
                .mode("append") \
                .option("replaceWhere", f"dt_ingest = '{self.execution_date}'") \
                .partitionBy("dt_ingest") \
                .save(path)
            self.logger.info(f"Dados gravados com sucesso em {path}")
        except Exception as e:
            self.logger.error(f"Erro ao gravar dados em {path}: {e}", exc_info=True)
            raise e

    def update_bronze_status(self):
        try:
            bronze_table = DeltaTable.forPath(self.spark, self.bronze_path)
            bronze_table.update(
                condition=F.expr(f"dt_ingest = '{self.execution_date}' AND silver_processed = false"),
                set={"silver_processed": F.lit(True)}
            )
            self.logger.info("Status 'silver_processed' atualizado na camada Bronze com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao atualizar status na camada Bronze: {e}", exc_info=True)
            raise e


if __name__ == "__main__":
    spark = get_spark_session("Bronze To Silver Bucket Ingest")
    info = parse_args()
    contract = get_contract(info.table)
    qi = QualityAndIngest(spark, info, contract)

    dataframe = qi.get_dataframe()
    dataframe.show(10)

    if not dataframe.isEmpty():
        df_validos, df_erros = qi.validate_data(dataframe)

        if not df_validos.isEmpty():
            qi.logger.info("Gravando registros válidos na Silver.")
            qi.write_delta(df_validos, qi.silver_path)
        
        if not df_erros.isEmpty():
            qi.logger.info("Gravando registros com erro na Quarentena.")
            qi.write_delta(df_erros, qi.quarantine_path)
        
        qi.update_bronze_status()
        
    else:
        qi.logger.info(f"Nenhum dado novo para processar em {info.execution_date}.")