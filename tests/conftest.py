# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Pytest fixture to create a shared SparkSession for testing.
    Scope is 'session' to avoid recreating the session for each test.
    """
    session = (
        SparkSession.builder.appName("PySparkTest")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    yield session
    session.stop()
