from __future__ import annotations
import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "DataPipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.executor.instances", "2")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "2")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.eventLog.dir", "file:/opt/spark/spark-events")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )