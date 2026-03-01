from pyspark.sql import SparkSession
from urllib.parse import urlparse
from settings import Minio, SparkConfig, SparkTuningConfig

def _minio_endpoint_for_spark():
    host = Minio.HOST
    if host.startswith("http"):
        parsed = urlparse(host)
        host = parsed.hostname
    return f"{host}:{Minio.PORT}"


def spark_connect():
    endpoint = _minio_endpoint_for_spark()
    return (
        SparkSession.builder
        .appName("MinioDataProcessing")
        .master(f"spark://{SparkConfig.HOST}:{SparkConfig.PORT}")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", Minio.USER)
        .config("spark.hadoop.fs.s3a.secret.key", Minio.PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.memory", SparkTuningConfig.DRIVER_MEMORY)
        .config("spark.executor.memory", SparkTuningConfig.EXECUTOR_MEMORY)
        .config("spark.executor.cores", SparkTuningConfig.EXECUTOR_CORES)
        .config("spark.executor.instances", SparkTuningConfig.EXECUTOR_INSTANCES)
        .config("spark.sql.shuffle.partitions", SparkTuningConfig.SHUFFLE_PARTITIONS)
        .getOrCreate()
    )

