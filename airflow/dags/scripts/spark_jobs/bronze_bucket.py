from __future__ import annotations

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException

from spark_config import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
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
        log.info("Lendo arquivos de: %s", raw_path)
        df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)
        
        if len(df.head(1)) == 0:
            log.warning("Nenhum dado encontrado para %s em %s. Pipeline encerrado com sucesso (sem dados).", dataset, raw_path)
            return

        spark.sparkContext.setJobGroup(f"write_{dataset}", f"Writing {dataset} (Partitioned)")
        
        df_with_date = df.withColumn("dt_ingestao", lit(execution_date))
        log.info("Gravando %s via Delta Lake (partition dt_ingestao) → %s", dataset, delta_path)
        spark.sparkContext.setLogLevel("WARN")
        
        df_with_date.write.format("delta") \
            .mode("overwrite") \
            .option("partitionOverwriteMode", "dynamic") \
            .partitionBy("dt_ingestao") \
            .save(delta_path)
        
        if zorder_col and zorder_col in df.columns:
            log.info("Executando OPTIMIZE com ZORDER BY (%s)...", zorder_col)
            spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY ({zorder_col})")
        else:
            log.info("Executando OPTIMIZE (Compactação de pequenos arquivos apenas)...")
            spark.sql(f"OPTIMIZE delta.`{delta_path}`")
            
        log.info("✅ Sucesso! Dataset %s processado e otimizado no Delta.", dataset)

    except AnalysisException as e:
        log.error("Erro de análise (caminho não existe ou schema inválido): %s", e)
        raise
    except Exception as e:
        log.error("Erro inesperado durante a gravação Delta: %s", e)
        raise


if __name__ == "__main__":
    if len(sys.argv) < 5:
        log.error(
            "Argumentos insuficientes.\n"
            "Uso: script.py <dataset> <raw_path_base> <delta_path> <execution_date> [zorder_col]"
        )
        sys.exit(1)

    dataset = sys.argv[1]
    raw_path_csv = f"{sys.argv[2]}/*.csv"
    delta_path = sys.argv[3]
    execution_date = sys.argv[4]
    
    zorder_col = sys.argv[5] if len(sys.argv) > 5 else None

    spark = get_spark_session("Bronze Ingest — Delta Lake")

    try:
        process_and_save_delta(spark, dataset, raw_path_csv, delta_path, execution_date, zorder_col)
    finally:
        spark.stop()