# tests/bronze/test_ingest_postgres.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from spark_jobs.bronze.ingest_postgres import add_ingestion_timestamp

def test_add_ingestion_timestamp(spark: SparkSession):
    """
    Tests the add_ingestion_timestamp function to ensure it adds the
    'ingestion_tms' column with the correct type.
    
    :param spark: The SparkSession fixture from conftest.py.
    """
    # 1. Create a sample input DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("data", StringType(), True),
    ])
    input_data = [(1, "A"), (2, "B")]
    input_df = spark.createDataFrame(input_data, schema)
    
    # 2. Apply the transformation function
    output_df = add_ingestion_timestamp(input_df)
    
    # 3. Assertions
    # Check that the new column is present
    assert "ingestion_tms" in output_df.columns
    
    # Check that the number of columns is correct
    assert len(output_df.columns) == len(input_df.columns) + 1
    
    # Check the data type of the new column
    assert isinstance(output_df.schema["ingestion_tms"].dataType, TimestampType)
    
    # Check that the original data is intact
    assert output_df.count() == 2
    assert output_df.select("id", "data").collect() == input_df.collect()
