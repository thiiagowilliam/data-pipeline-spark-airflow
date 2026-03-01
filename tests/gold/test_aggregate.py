# tests/gold/test_aggregate.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, DecimalType
)
from datetime import date
from decimal import Decimal

from spark_jobs.gold.aggregate import transform

@pytest.fixture(scope="module")
def silver_df(spark: SparkSession):
    """Creates a sample Silver DataFrame for testing the gold aggregation."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("price", DecimalType(10, 2), False),
        StructField("qty", IntegerType(), False),
    ])
    input_data = [
        # CUST-A, Jan 2026: 2 orders, total sales = 100 + 50 = 150
        (1, "CUST-A", date(2026, 1, 15), Decimal("50.00"), 2),
        (2, "CUST-A", date(2026, 1, 20), Decimal("50.00"), 1),
        # CUST-B, Jan 2026: 1 order, total sales = 200
        (3, "CUST-B", date(2026, 1, 25), Decimal("200.00"), 1),
        # CUST-A, Feb 2026: 1 order, total sales = 300
        (4, "CUST-A", date(2026, 2, 1), Decimal("150.00"), 2),
    ]
    return spark.createDataFrame(input_data, schema)

def test_gold_aggregation(silver_df):
    """
    Tests the gold aggregation transform function.
    """
    # Apply the transformation
    gold_df = transform(silver_df)
    
    # --- Assertions ---
    
    # 1. Assert count: Should be 3 unique groups (CUST-A/Jan, CUST-B/Jan, CUST-A/Feb)
    assert gold_df.count() == 3
    
    # 2. Collect results and put into a dictionary for easy lookup
    results = {
        (row.customer_id, row.year_month): row for row in gold_df.collect()
    }
    
    # 3. Assert CUST-A, Jan 2026
    cust_a_jan = results.get(("CUST-A", "2026-01"))
    assert cust_a_jan is not None
    assert cust_a_jan.unique_orders == 2
    assert cust_a_jan.total_sales == pytest.approx(Decimal("150.00")) # (50*2) + 50
    
    # 4. Assert CUST-B, Jan 2026
    cust_b_jan = results.get(("CUST-B", "2026-01"))
    assert cust_b_jan is not None
    assert cust_b_jan.unique_orders == 1
    assert cust_b_jan.total_sales == pytest.approx(Decimal("200.00"))
    
    # 5. Assert CUST-A, Feb 2026
    cust_a_feb = results.get(("CUST-A", "2026-02"))
    assert cust_a_feb is not None
    assert cust_a_feb.unique_orders == 1
    assert cust_a_feb.total_sales == pytest.approx(Decimal("300.00")) # 150*2
