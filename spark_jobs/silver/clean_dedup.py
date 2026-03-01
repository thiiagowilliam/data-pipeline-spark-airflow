# spark_jobs/silver/clean_dedup.py
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, to_date, lit
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

def get_spark_session() -> SparkSession:
    """
    Initializes and returns a SparkSession configured for Delta Lake and S3 access.
    """
    return (
        SparkSession.builder
        .appName("SilverTransformation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def main():
    """
    Main function to run the Silver transformation process.
    - Reads data from the Bronze Delta table.
    - Cleans data (type casting, null handling).
    - Deduplicates records based on a unique key and ordering column.
    - Merges (upserts) the cleaned data into the Silver Delta table.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("INFO")

    # --- Read Configuration from Environment Variables ---
    bronze_path = os.getenv("BRONZE_DELTA_PATH", "s3a://bronze/sales")
    silver_path = os.getenv("SILVER_DELTA_PATH", "s3a://silver/sales")

    spark.log.info(f"Reading from Bronze table: {bronze_path}")

    # --- Read from Bronze Delta Lake ---
    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
    except Exception as e:
        spark.log.error(f"Failed to read from Bronze Delta table: {e}")
        raise

    # --- Data Cleaning and Transformation ---
    spark.log.info("Starting data cleaning and transformation.")
    
    # 1. Handle Nulls: Drop rows if primary identifiers are null
    df_cleaned = df_bronze.na.drop(subset=["id", "customer_id", "order_date"])

    # 2. Type Casting: Ensure correct data types
    df_cleaned = (
        df_cleaned.withColumn("price", col("price").cast(DecimalType(10, 2)))
                  .withColumn("qty", col("qty").cast("integer"))
                  .withColumn("order_date", to_date(col("order_date")))
    )
    
    # 3. Deduplication: Keep the latest record for each unique ID
    # The window is partitioned by the unique key 'id' and ordered by 'order_date'
    # to find the most recent record for each sale.
    window_spec = Window.partitionBy("id").orderBy(col("order_date").desc())
    df_deduplicated = (
        df_cleaned.withColumn("row_num", row_number().over(window_spec))
                  .filter(col("row_num") == 1)
                  .drop("row_num")
    )
    
    source_count = df_deduplicated.count()
    spark.log.info(f"Source DataFrame has {source_count} rows after cleaning and deduplication.")

    # --- Merge (Upsert) into Silver Delta Lake ---
    spark.log.info(f"Merging data into Silver Delta table at {silver_path}")

    # Check if the target Delta table exists. If not, create it.
    if not DeltaTable.isDeltaTable(spark, silver_path):
        spark.log.info("Silver Delta table does not exist. Creating it.")
        (
            df_deduplicated.write.format("delta")
            .partitionBy("order_date") # Partitioning by date is good for time-series queries
            .save(silver_path)
        )
        spark.log.info("Successfully created and populated the Silver Delta table.")
    else:
        spark.log.info("Silver Delta table exists. Merging new data.")
        silver_table = DeltaTable.forPath(spark, silver_path)
        
        # The MERGE operation provides idempotency. Rerunning the job with the same
        # source data will not create duplicates.
        try:
            (
                silver_table.alias("target")
                .merge(
                    df_deduplicated.alias("source"),
                    "target.id = source.id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            spark.log.info("Successfully merged data into the Silver Delta table.")
        except Exception as e:
            spark.log.error(f"Failed to merge data into Silver table: {e}")
            raise

    spark.stop()

if __name__ == "__main__":
    main()
