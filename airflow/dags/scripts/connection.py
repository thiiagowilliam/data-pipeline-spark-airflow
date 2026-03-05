# /opt/airflow/dags/scripts/etl_job.py
from pyspark.sql import SparkSession
from settings import Minio
import psycopg2
from psycopg2 import OperationalError
import boto3


def spark_connect(Minio) -> SparkSession:
    spark = SparkSession.builder \
        .appName("MinIO S3A Integration") \
        .getOrCreate()
    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.access.key", Minio.USER)
    hconf.set("fs.s3a.secret.key", Minio.PASSWORD)
    hconf.set("fs.s3a.endpoint", f"http://{Minio.HOST}:{Minio.PORT}")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return spark

def database_connect(Database):
    try:
        connection = psycopg2.connect(
            host=Database.HOST,
            database=Database.TABLE,
            user=Database.USER,
            password=Database.PASSWORD,
            port=Database.PORT
        )
        print(f"Conectado ao banco '{Database.TABLE}' com sucesso!")
        return connection
    except OperationalError as e:
        print(f"Erro ao conectar ao banco: {e}")
        return None

def bucket_connect(Minio):
    try:
        return  boto3.client(
            "s3",
            endpoint_url=Minio.HOST,
            aws_access_key_id=Minio.USER,
            aws_secret_access_key=Minio.PASSWORD,
        )
    except:
        return None


def main():
    spark = spark_connect(Minio)
    file_path = "s3a://datalake/raw/*.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df.show()

if __name__ == "__main__":
    main()