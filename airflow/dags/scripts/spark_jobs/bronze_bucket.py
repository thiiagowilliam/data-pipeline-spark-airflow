from __future__ import annotations

import sys
import logging
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException

from spark_config import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("bronze_delta")

def process_and_save_delta(
    spark: SparkSession, 
    dataset: str, 
    raw_path: str, 
    delta_path: str, 
    execution_date: str, 
    zorder_col: str | None = None
) -> None:
    try:
        log.info(f"Lendo arquivos CSV de: {raw_path}")
        df = spark.read.option("header", True).csv(raw_path)
        
        if df.isEmpty():
            log.warning(f"Nenhum dado encontrado para {dataset} em {raw_path}. Pipeline encerrado com sucesso.")
            return
        
        spark.sparkContext.setJobGroup(f"write_{dataset}", f"Writing {dataset} (Partitioned)")
        df_with_date = df.withColumn("dt_ingestao", lit(execution_date))

        log.info(f"Gravando {dataset} via Delta Lake (partition dt_ingestao) → {delta_path}")

        spark.sparkContext.setLogLevel("WARN")

        df_with_date.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("dt_ingestao") \
            .option("mergeSchema", "true") \
            .option("replaceWhere", f"dt_ingestao = '{execution_date}'") \
            .save(delta_path)
        
        optimize_query = f"OPTIMIZE delta.`{delta_path}` WHERE dt_ingestao = '{execution_date}'"
        
        if zorder_col and zorder_col in df_with_date.columns:
            log.info(f"Executando OPTIMIZE da partição com ZORDER BY ({zorder_col})...")
            spark.sql(f"{optimize_query} ZORDER BY ({zorder_col})")
        else:
            log.info("Executando OPTIMIZE da partição (Compactação de pequenos arquivos apenas)...")
            spark.sql(optimize_query)
            
        log.info(f"Dataset {dataset} processado e otimizado no Delta.")

    except AnalysisException as e:
        log.error(f"Erro de análise (caminho não existe ou schema inválido): {e}")
        raise
    except Exception as e:
        log.error(f"Erro inesperado durante a gravação Delta: {e}")
        raise

def parse_args():
    parser = argparse.ArgumentParser(description="Pipeline Raw to Bronze via PySpark")
    parser.add_argument("--datalake", required=False, help="Nome do bucket/datalake")
    parser.add_argument("--raw_path", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--delta_path", required=True)
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--zorder_col", required=False, default=None, help="Coluna para otimizar com ZORDER")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("Bronze Ingest — Delta Lake")
    try:
        process_and_save_delta(
            spark=spark, 
            dataset=args.dataset, 
            raw_path=args.raw_path, 
            delta_path=args.delta_path, 
            execution_date=args.execution_date, 
            zorder_col=args.zorder_col
        )
    except Exception as e:
        log.critical(f"O Job falhou e será encerrado. Erro: {e}")
        sys.exit(1)
    finally:
        log.info("Encerrando Spark Session.")
        spark.stop()