import os
class Minio:
    HOST = os.getenv("MINIO_HOST", "http://minio")
    PORT = int(os.getenv("MINIO_PORT", "9000"))
    BUCKET_NAME = os.getenv("MINIO_BUCKET")
    USER = os.getenv("MINIO_USER")
    PASSWORD = os.getenv("MINIO_PASSWORD")
    CONN_ID = os.getenv("MINIO_CONN_ID", "aws_default")

class Database:
    HOST = os.getenv("HOST", "postgres")
    NAME = os.getenv("NAME", "origem_db")
    USER = os.getenv("USER", "admin")
    PASSWORD = os.getenv("PASSWORD", "admin")
    PORT = os.getenv("PORT", 5432)

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