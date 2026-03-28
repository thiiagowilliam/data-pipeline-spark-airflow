from __future__ import annotations

import sys
import logging
import argparse
from dataclasses import dataclass

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

from spark_config import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("bronze_delta")

@dataclass
class JobArgs:
    datalake: str
    table: str
    layer: str
    execution_date: str

class ProcessData:
    def __init__(self, args: JobArgs, spark: SparkSession):
        self.spark = spark
        self.datalake = args.datalake
        self.table = args.table
        self.layer = args.layer
        self.execution_date = args.execution_date
        self.checkpoint = f"s3a://{self.datalake}/checkpoints/raw_to_bronze/{self.table}"

    def read_csv(self) -> DataFrame:
        raw_path = f"s3a://{self.datalake}/raw/{self.table}/"
        log.info(f"LENDO: {raw_path}")
        self.spark.conf.set("spark.sql.streaming.schemaInference", "true")
        return (self.spark.readStream
            .format("csv")
            .option("header", "true")
            .load(raw_path))
        
    def delta_write(self, dataframe: DataFrame):
        layer_path = f"s3a://{self.datalake}/{self.layer}/{self.table}"
        
        df_transformed = dataframe.withColumn("dt_ingest", F.to_date(F.lit(self.execution_date)))
        log.info(f"Escrevendo via Streaming (AvailableNow) em: {layer_path}")
        try:
            query = (df_transformed.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", self.checkpoint)
                .trigger(availableNow=True)
                .start(layer_path)
            )
            
            query.awaitTermination() 
            log.info("Gravação do micro-batch concluída com sucesso.")
            
        except Exception as error:
            log.error(f"Falha crítica ao gravar fluxo em {layer_path}: {error}", exc_info=True) 
            raise error
        

def parse_args() -> JobArgs:
    parser = argparse.ArgumentParser(description="Bronze Ingest — CSV → Delta Lake")
    parser.add_argument("--datalake", required=True, help="Nome do bucket MinIO/S3")
    parser.add_argument("--table", required=True, help="Nome da tabela")
    parser.add_argument("--layer", required=True, help="Camada (ex: bronze)")
    parser.add_argument("--execution_date", required=True, help="Data de execução (YYYY-MM-DD)")
    parsed = parser.parse_args()
    return JobArgs(
        datalake=parsed.datalake,
        table=parsed.table,
        layer=parsed.layer,
        execution_date=parsed.execution_date
    )


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session(f"Bronze_Ingest_{args.table}")
    try:
        process_data = ProcessData(args, spark)
        dataframe = process_data.read_csv()
        process_data.delta_write(dataframe)
            
    except Exception as e:
        log.critical(f"Job Bronze falhou: {e}", exc_info=True)
        sys.exit(1)
    finally:
        log.info("Encerrando Spark Session.")
        spark.stop()