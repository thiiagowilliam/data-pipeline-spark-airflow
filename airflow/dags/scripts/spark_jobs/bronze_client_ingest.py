import os
import sys
from pyspark.sql import SparkSession
from typing import List, Tuple

def get_spark_session() -> SparkSession:
    """Creates and configures a SparkSession for Delta Lake."""
    builder = (
        SparkSession.builder.appName("MinIO S3A Integration with Delta")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return builder.getOrCreate()

def detect_dataset(file_path: str) -> str:
    if "clientes" in file_path:
        return "clientes"
    if "vendas" in file_path:
        return "vendas"
    raise ValueError(f"Dataset not identified in path: {file_path}")


def save_delta(spark: SparkSession, files_path: List[str], output_bucket: str = "datalake/bronze") -> List[Tuple[str, str]]:
    delta_paths = []
    for file_path in files_path:
        dataset = detect_dataset(file_path)
        file_id = file_path.split("/")[-1].replace(".csv", "")
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        delta_path = f"s3a://{output_bucket}/{dataset}/{file_id}"
        df.write.format("delta").mode("overwrite").save(delta_path)
        print(f"{file_path} -> {delta_path}")
        delta_paths.append((delta_path, dataset))
    return delta_paths


def insert_database(spark: SparkSession, delta_files):
    db_host = os.getenv("DB_HOST", "postgres")
    db_name = os.getenv("DB_NAME", "airflow")
    db_user = os.getenv("DB_USER", "airflow")
    db_password = os.getenv("DB_PASSWORD", "airflow")
    db_port = os.getenv("DB_PORT", "5432")
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    
    for delta_path, dataset in delta_files:
        df = spark.read.format("delta").load(delta_path)
        df = df.dropDuplicates(["id"])
        (
            df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"{dataset}_staging")
            .option("user", db_user)
            .option("password", db_password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

        print(f"{delta_path} inserted into staging {dataset}_staging")

if __name__ == "__main__":
    files_to_process = sys.argv[1:]
    if not files_to_process:
        print("No files received from Airflow. Exiting.")
        sys.exit(0)

    spark_session = get_spark_session()
    saved_delta_files = save_delta(spark_session, files_to_process)
    insert_database(spark_session, saved_delta_files)
    spark_session.stop()
