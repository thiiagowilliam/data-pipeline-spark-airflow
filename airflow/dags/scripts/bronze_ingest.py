from pyspark.sql import SparkSession
from datetime import datetime
from settings import Database, SparkConfig
import sys
import os


def get_spark():
    return (
        SparkSession.builder
        .appName("bronze_ingest")
        .master(f"spark://{SparkConfig.HOST}:{SparkConfig.PORT}")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


def process_file(files):
    spark = get_spark()

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(files)
    )

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    df.write.mode("overwrite").parquet(f"/app/data/bronze/{timestamp}")

    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{Database.HOST}:{Database.PORT}/{Database.NAME}") \
        .option("dbtable", "bronze_clientes") \
        .option("user", Database.USER) \
        .option("password", Database.PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    spark.stop()


if __name__ == "__main__":
    files = sys.argv[1:]
    process_file(files)