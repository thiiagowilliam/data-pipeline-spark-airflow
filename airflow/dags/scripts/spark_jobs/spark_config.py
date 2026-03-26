from __future__ import annotations
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "DataPipeline") -> SparkSession:
    packages = [
        "io.delta:delta-spark_2.13:4.1.0",
        "org.apache.hadoop:hadoop-aws:3.4.1",
        "software.amazon.awssdk:bundle:2.30.0"
    ]
    packages_str = ",".join(packages)

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages_str)
        
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        .getOrCreate()
    )