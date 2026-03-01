# spark_jobs/bronze/ingest_postgres.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def get_spark_session() -> SparkSession:
    """
    Initializes and returns a SparkSession configured for Delta Lake and S3 access.
    Configuration details for S3 (MinIO) are expected to be set in the Spark configuration
    provided by the Spark-on-Kubernetes operator (via YAML).
    """
    return (
        SparkSession.builder
        .appName("BronzeIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def main():
    """
    Main function to run the Bronze ingestion process.
    - Reads data from a PostgreSQL table.
    - Adds an ingestion timestamp.
    - Writes the data to a Bronze Delta table in MinIO.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("INFO")

    # --- Read Configuration from Environment Variables ---
    # These are injected into the pod by Kubernetes via Secrets and ConfigMaps
    pg_host = os.getenv("POSTGRES_HOST")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_db = os.getenv("POSTGRES_DB")
    pg_table = os.getenv("POSTGRES_TABLE", "public.sales") # Assuming schema.table
    
    bronze_path = os.getenv("BRONZE_DELTA_PATH", "s3a://bronze/sales")

    if not all([pg_host, pg_user, pg_password, pg_db]):
        raise ValueError("One or more PostgreSQL environment variables are not set.")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    
    spark.log.info(f"Reading from PostgreSQL table: {pg_table} at {jdbc_url}")

    # --- Read from PostgreSQL ---
    try:
        df_raw = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", pg_table)
            .option("user", pg_user)
            .option("password", pg_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
    except Exception as e:
        spark.log.error(f"Failed to read from PostgreSQL: {e}")
        raise

    # --- Add Ingestion Timestamp ---
    df_bronze = df_raw.withColumn("ingestion_tms", current_timestamp())

    spark.log.info(f"Writing {df_bronze.count()} rows to Bronze Delta table at {bronze_path}")

    # --- Write to Bronze Delta Lake ---
    # Using 'overwrite' mode to ensure idempotency for this daily batch job.
    # For streaming or CDC, you would typically use 'append'.
    try:
        (
            df_bronze.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true") # Good practice for schema evolution
            .save(bronze_path)
        )
        spark.log.info("Successfully wrote to Bronze Delta table.")
    except Exception as e:
        spark.log.error(f"Failed to write to Bronze Delta table: {e}")
        raise

    spark.stop()

if __name__ == "__main__":
    main()
