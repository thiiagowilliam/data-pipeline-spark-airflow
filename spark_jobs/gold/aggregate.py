# spark_jobs/gold/aggregate.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, countDistinct, date_format

def get_spark_session() -> SparkSession:
    """
    Initializes and returns a SparkSession configured for Delta Lake and S3 access.
    """
    return (
        SparkSession.builder
        .appName("GoldAggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def main():
    """
    Main function to run the Gold aggregation process.
    - Reads cleaned data from the Silver Delta table.
    - Performs business-level aggregations (e.g., monthly sales per customer).
    - Writes the aggregated data to a Gold Delta table, partitioned for performance.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("INFO")

    # --- Read Configuration from Environment Variables ---
    silver_path = os.getenv("SILVER_DELTA_PATH", "s3a://silver/sales")
    gold_path = os.getenv("GOLD_DELTA_PATH", "s3a://gold/monthly_sales_by_customer")

    spark.log.info(f"Reading from Silver table: {silver_path}")

    # --- Read from Silver Delta Lake ---
    try:
        df_silver = spark.read.format("delta").load(silver_path)
    except Exception as e:
        spark.log.error(f"Failed to read from Silver Delta table: {e}")
        raise

    # --- Business Aggregation ---
    spark.log.info("Starting business-level aggregation.")
    
    # Create helper columns for aggregation
    df_agg_source = df_silver.withColumn("total_price", col("qty") * col("price")) \
                             .withColumn("year_month", date_format(col("order_date"), "yyyy-MM"))

    # Group by customer and month to calculate total sales and order count
    df_gold = (
        df_agg_source.groupBy("customer_id", "year_month")
        .agg(
            spark_sum("total_price").alias("total_sales"),
            countDistinct("id").alias("unique_orders")
        )
        .orderBy(col("year_month").desc(), col("customer_id"))
    )
    
    final_count = df_gold.count()
    spark.log.info(f"Gold DataFrame has {final_count} rows after aggregation.")

    # --- Write to Gold Delta Lake ---
    # Overwriting the entire table is a simple, idempotent strategy for daily aggregations.
    # Partitioning by 'year_month' is crucial for efficient querying of the Gold table.
    spark.log.info(f"Writing aggregated data to Gold Delta table at {gold_path}")
    
    try:
        (
            df_gold.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year_month")
            .save(gold_path)
        )
        spark.log.info("Successfully wrote to Gold Delta table.")
    except Exception as e:
        spark.log.error(f"Failed to write to Gold table: {e}")
        raise

    spark.stop()


if __name__ == "__main__":
    main()
