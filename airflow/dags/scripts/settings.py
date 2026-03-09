import os

class Minio:
    HOST = os.getenv("MINIO_HOST", "minio")
    PORT = int(os.getenv("MINIO_PORT", "9000"))
    BUCKET_NAME = os.getenv("MINIO_BUCKET", "datalake")
    USER = os.getenv("MINIO_USER", "minioadmin")
    PASSWORD = os.getenv("MINIO_PASSWORD", "minioadmin")
    CONN_ID = os.getenv("MINIO_CONN_ID", "aws_default")

class Database:
    HOST = os.getenv("DB_HOST", "postgres")
    NAME = os.getenv("DB_NAME", "airflow")
    USER = os.getenv("DB_USER", "airflow")
    PASSWORD = os.getenv("DB_PASSWORD", "airflow")
    PORT = os.getenv("DB_PORT", 5432)

class SparkConfig:
    HOST = os.getenv("SPARK_HOST", "spark-master")
    PORT = int(os.getenv("SPARK_PORT", "7077"))

class SparkTuningConfig:
    DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "1g")
    EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "1g")
    EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "1")
    EXECUTOR_INSTANCES = os.getenv("SPARK_EXECUTOR_INSTANCES", "2")
    SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")

class DataLakeZones:
    RAW = "raw/"
    BRONZE = "bronze/"
    SILVER = "silver/"
    GOLD = "gold/"
    PROCESSED = "processed/"