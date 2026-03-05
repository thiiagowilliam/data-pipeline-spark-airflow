import sys
from settings import Minio, Database
from connection import spark_connect
from pyspark.sql import SparkSession

def save_parquet(minio: Minio, files_path: list[str], output_bucket: str = "datalake/bronze"):
    spark = spark_connect(minio)
    files_id = [file.split('/')[-1].replace(".csv", "") for file in files_path]
    
    for file_path, file_id in zip(files_path, files_id):
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        parquet_path = f"s3a://{output_bucket}/{file_id}"
        df.write.mode("overwrite").parquet(parquet_path)
        print(f"Arquivo {file_path} salvo como Parquet em {parquet_path}")

    return [f"s3a://{output_bucket}/{fid}" for fid in files_id]


def insert_database(spark: SparkSession, files_path: list[str], database: Database, table_name: str):
    for file_path in files_path:
        df = spark.read.parquet(file_path)
        df.write \
          .format("jdbc") \
          .option("url", f"jdbc:postgresql://{database.HOST}:{database.PORT}/{database.NAME}") \
          .option("dbtable", table_name) \
          .option("user", database.USER) \
          .option("password", database.PASSWORD) \
          .mode("append") \
          .save()
        print(f"Dados do Parquet {file_path} inseridos na tabela {table_name}")

if __name__ == "__main__":
    files_to_process = sys.argv[1:]
    
    if not files_to_process:
        print("Nenhum arquivo recebido do Airflow. Encerrando.")
        sys.exit(0)

    spark = spark_connect(Minio)
    parquet_files = save_parquet(Minio, files_to_process)

    insert_database(spark, parquet_files, Database, table_name="airflow_file_data")