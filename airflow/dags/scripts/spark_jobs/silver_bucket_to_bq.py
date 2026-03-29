from __future__ import annotations

import sys
import os
import json
import uuid
import logging
import base64
import argparse
from typing import Optional, List
from dataclasses import dataclass
from pydantic import BaseModel, Field as PydanticField
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.storagelevel import StorageLevel

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
    datalake: str
    layer: str
    table: str
    execution_date: str
    run_id: str = ""
    
    @property
    def full_table_id(self) -> str:
        return f"{self.project_id}.{self.layer}.{self.table}"

    @property
    def input_path(self) -> str:
        return f"s3a://{self.datalake}/silver/{self.table}"
        
    @property
    def checkpoint_path(self) -> str:
        return f"s3a://{self.datalake}/checkpoints/silver_bucket_to_bq/{self.table}"


class BigQueryLoadEngine:
    def __init__(self, spark: SparkSession, config: BigQueryLoadConfig):
        self.spark = spark
        self.config = config
        
        base_logger = logging.getLogger("BQ_Engine")
        self.logger = RunIdAdapter(base_logger, {"run_id": self.config.run_id})
        self._apply_tuning()

    def _apply_tuning(self):
        conf = self.spark.conf
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.sql.shuffle.partitions", "200")
        conf.set("viewsEnabled", "true")
        conf.set("materializationDataset", self.config.layer) 
        self.logger.info("Tuning aplicado para leitura Delta e escrita BigQuery")
    

    def _process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        self.logger.info(f"Iniciando micro-batch ID: {batch_id}")
        
        if batch_df.isEmpty():
            self.logger.info("Micro-batch vazio → pulando")
            return

        batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        
        try:
            self.logger.info(f"Efetuando LOAD no BigQuery: {self.config.full_table_id}")
            
            writer = (batch_df.write
                .format("bigquery")
                .option("table", self.config.full_table_id)
                .option("parentProject", self.config.project_id)
                .option("writeMethod", "direct") 
                .mode("append"))

            gcp_json_str = os.environ.get("GCP_KEY_JSON")
            if gcp_json_str:
                b64_creds = base64.b64encode(gcp_json_str.encode('utf-8')).decode('utf-8')
                writer = writer.option("credentials", b64_creds)
                self.logger.info("Credenciais do Airflow aplicadas com sucesso via Base64.")
                
            writer.save()
                
        except Exception as e:
            self.logger.error(f"Falha ao gravar no BigQuery: {e}")
            raise
        finally:
            batch_df.unpersist()
            self.logger.info(f"Micro-batch {batch_id} finalizado e memória liberada.")

    def execute_pipeline(self):
        self.logger.info(f"Extraindo dados contínuos da Silver: {self.config.input_path}")
        try:
            df_stream = self.spark.readStream.format("delta").load(self.config.input_path)
            df_stream = df_stream.withColumn("_bq_run_id", F.lit(self.config.run_id)) \
                .withColumn("_bq_loaded_at", F.current_timestamp())
            
            query = (df_stream.writeStream
                     .foreachBatch(self._process_batch)
                     .option("checkpointLocation", self.config.checkpoint_path)
                     .trigger(availableNow=True)
                     .start())

            query.awaitTermination()
            self.logger.info("Processo de carga no BigQuery concluído com sucesso!")
            
        except Exception as e:
            self.logger.error("Falha crítica no pipeline de Streaming", exc_info=True)
            raise


def parse_args() -> tuple[BigQueryLoadConfig, str]:
    parser = argparse.ArgumentParser(description="Pipeline Silver -> BigQuery com PII (Streaming)")
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--datalake", required=True)
    parser.add_argument("--layer", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--execution_date", required=True)
    args = parser.parse_args()
    
    run_id = str(uuid.uuid4())[:8]
    config = BigQueryLoadConfig(
        project_id=args.project_id,
        datalake=args.datalake,
        layer=args.layer,
        table=args.table,
        execution_date=args.execution_date,
        run_id=run_id
    )
    return config


if __name__ == "__main__":
    config = parse_args()
    
    base_main_logger = logging.getLogger("Main_BQ_Pipeline")
    main_logger = RunIdAdapter(base_main_logger, {"run_id": config.run_id})
    
    spark = get_spark_session(f"BQ_Load_{config.table}_{config.run_id}")
    
    try:
        engine = BigQueryLoadEngine(spark=spark, config=config)
        engine.execute_pipeline()
        
    except Exception as e:
        main_logger.critical(f"Falha catastrófica: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()