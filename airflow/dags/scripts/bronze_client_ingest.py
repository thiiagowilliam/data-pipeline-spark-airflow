import sys
from settings import Minio, Database
from connection import spark_connect
from pyspark.sql import SparkSession


def detect_dataset(file_path: str):

    if "clientes" in file_path:
        return "clientes"

    if "vendas" in file_path:
        return "vendas"

    raise ValueError(f"Dataset não identificado no caminho: {file_path}")


def save_parquet(minio: Minio, files_path: list[str], output_bucket: str = "datalake/bronze"):

    spark = spark_connect(minio)

    parquet_paths = []

    for file_path in files_path:

        dataset = detect_dataset(file_path)

        file_id = file_path.split("/")[-1].replace(".csv", "")

        df = spark.read.csv(file_path, header=True, inferSchema=True)

        parquet_path = f"s3a://{output_bucket}/{dataset}/{file_id}"

        df.write.mode("overwrite").parquet(parquet_path)

        print(f"{file_path} → {parquet_path}")

        parquet_paths.append((parquet_path, dataset))

    return parquet_paths


def insert_database(
    spark: SparkSession,
    parquet_files: list[tuple[str, str]],
    database: Database
):

    for parquet_path, dataset in parquet_files:

        table_name = dataset  # clientes ou vendas

        df = spark.read.parquet(parquet_path)

        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{database.HOST}:{database.PORT}/{database.NAME}") \
            .option("dbtable", table_name) \
            .option("user", database.USER) \
            .option("password", database.PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"{parquet_path} inserido na tabela {table_name}")


if __name__ == "__main__":

    files_to_process = sys.argv[1:]

    if not files_to_process:
        print("Nenhum arquivo recebido do Airflow. Encerrando.")
        sys.exit(0)

    spark = spark_connect(Minio)

    parquet_files = save_parquet(Minio, files_to_process)

    insert_database(spark, parquet_files, Database)