"""
Configuração centralizada de SparkSession para o Data Pipeline.

Fornece uma factory para criação de SparkSession com configuração
MinIO/S3A e Delta Lake, eliminando duplicação entre os Spark jobs.

Uso:
    from spark_config import get_spark_session
    spark = get_spark_session("My App Name")
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "DataPipeline") -> SparkSession:
    """
    Cria uma SparkSession configurada para MinIO/S3A e Delta Lake.

    Args:
        app_name: Nome da aplicação exibido no Spark UI.

    Returns:
        SparkSession configurada e pronta para uso.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("MINIO_ENDPOINT"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.eventLog.dir", "file:/opt/spark/spark-events")
        .getOrCreate()
    )
