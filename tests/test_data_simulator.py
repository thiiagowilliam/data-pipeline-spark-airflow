import os
import boto3
import pandas as pd
import pytest
from moto import mock_s3
from faker import Faker
from simulator.data_simulator import DataGenerator, MinioUploader

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture
def s3_client(aws_credentials):
    """S3 client mocked with moto."""
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")

def test_data_generator():
    """Tests the DataGenerator class."""
    fake = Faker()
    generator = DataGenerator(fake)
    df_clients = generator.generate_clients(10)
    df_sales = generator.generate_sales(10)

    assert isinstance(df_clients, pd.DataFrame)
    assert len(df_clients) == 10
    assert "nome" in df_clients.columns

    assert isinstance(df_sales, pd.DataFrame)
    assert len(df_sales) == 10
    assert "valor_total" in df_sales.columns

def test_minio_uploader(s3_client):
    """Tests the MinioUploader class."""
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    uploader = MinioUploader(
        endpoint_url="https://s3.us-east-1.amazonaws.com",
        access_key="testing",
        secret_key="testing",
        bucket_name=bucket_name,
    )
    uploader.s3_client = s3_client

    data = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data)

    uploader.upload_to_minio(df, "test-folder", "test-file.csv")

    response = s3_client.get_object(Bucket=bucket_name, Key="raw/test-folder/test-file.csv")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
