from __future__ import annotations

import sys
import logging
import argparse
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

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
    
    def read_csv(self) -> DataFrame | None:
        raw_path = f"s3a://{self.datalake}/raw/{self.table}/*.csv"
        log.info(f"Tentando ler CSVs de: {raw_path}")
        try:
            return self.spark.read.csv(raw_path, header=True)
        except AnalysisException as ae:
            if "PATH_NOT_FOUND" in str(ae):
                log.warning(f"Caminho não encontrado: {raw_path}. Nenhum arquivo CSV para processar hoje.")
                return None
            raise ae
        
    def delta_write(self, dataframe: DataFrame):
        layer_path = f"s3a://{self.datalake}/{self.layer}/{self.table}"
        df_transformed = dataframe.withColumns({
            "dt_ingest": F.to_date(F.lit(self.execution_date)),
            "silver_processed": F.lit(False)
        })

        log.info(f"Escrevendo Bronze: {layer_path}")
        try:
            df_transformed.write.format("delta") \
                .mode("overwrite") \
                .option("replaceWhere", f"dt_ingest = '{self.execution_date}'") \
                .partitionBy("dt_ingest") \
                .save(layer_path)
                    
            log.info("Gravação na camada Bronze concluída com sucesso.")
        except InterruptedError as error:
            log.info(f"Erro ao gravar em {layer_path}.")
            return error
        

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
        if len(dataframe.head(1)) > 0:
            process_data.delta_write(dataframe)
        else:
            log.error("DataFrame vazio ou inexistente. Pulando a gravação.")
            
    except Exception as e:
        log.critical(f"Job Bronze falhou: {e}", exc_info=True)
        sys.exit(1)
    finally:
        log.info("Encerrando Spark Session.")
        spark.stop()