# tests/silver/test_clean_dedup.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType, DecimalType
)
from datetime import date

from spark_jobs.silver.clean_dedup import transform

@pytest.fixture(scope="module")
def messy_df(spark: SparkSession):
    """Creates a messy DataFrame for testing the silver transformation."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("price", StringType(), True),
        StructField("qty", StringType(), True),
    ])
    input_data = [
        (1, "CUST-A", "2026-01-15", "100.50", "2"),      # Valid record
        (2, "CUST-B", "2026-01-16", "200.00", "1"),      # Valid record
        (1, "CUST-A", "2026-01-14", "99.00", "2"),       # Duplicate of id=1, older
        (3, None, "2026-01-17", "300.00", "3"),         # Null customer_id
        (4, "CUST-D", None, "400.00", "4"),             # Null order_date
    ]
    return spark.createDataFrame(input_data, schema)


def test_silver_transform(spark: SparkSession, messy_df):
    """
    Tests the main silver transform function, including cleaning,
    type casting, and deduplication.
    """
    # Apply the transformation
    transformed_df = transform(messy_df)
    
    # --- Assertions ---
    
    # 1. Assert count: Should drop nulls and one duplicate
    # Initial: 5 rows -> Drop 2 nulls -> 3 rows -> Drop 1 duplicate -> 2 rows
    assert transformed_df.count() == 2
    
    # 2. Assert schema: Check for correct data types
    expected_schema = {
        "id": IntegerType(),
        "customer_id": StringType(),
        "order_date": DateType(),
        "price": DecimalType(10, 2),
        "qty": IntegerType(),
    }
    for field in transformed_df.schema:
        assert field.name in expected_schema
        assert isinstance(field.dataType, type(expected_schema[field.name]))
        
    # 3. Assert deduplication: Check that the latest record for id=1 was kept
    latest_record = transformed_df.filter("id = 1").first()
    assert latest_record is not None
    assert latest_record["order_date"] == date(2026, 1, 15)
    assert latest_record["price"] == pytest.approx(100.50)

    # 4. Collect and verify all data
    results = {row.id: row for row in transformed_df.collect()}
    assert 1 in results
    assert 2 in results
    assert results[1].customer_id == "CUST-A"
    assert results[2].customer_id == "CUST-B"
