from airflow import DAG
from datetime import datetime

from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator


def configure_batch_definition(context):

    data_source = context.data_sources.add_pandas(name="minio_pandas")

    data_asset = data_source.add_dataframe_asset(
        name="csv_asset"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="csv_batch"
    )

    import pandas as pd

    df = pd.read_csv(
        "s3://meu-bucket/dados/clientes.csv",
        storage_options={"conn_id": "aws_default"},
    )

    batch_definition.add_batch(dataframe=df)

    return batch_definition


def configure_expectations(context):
    import great_expectations.expectations as gxe
    from great_expectations import ExpectationSuite

    return context.suites.add_or_update(
        ExpectationSuite(
            name="csv_suite",
            expectations=[
                gxe.ExpectColumnValuesToNotBeNull(column="id"),
                gxe.ExpectColumnValuesToNotBeNull(column="name"),
                gxe.ExpectColumnValuesToBeBetween(
                    column="age",
                    min_value=0,
                    max_value=120
                ),
            ],
        )
    )


with DAG(
    dag_id="gx_validate_minio_csv",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    validate_csv = GXValidateBatchOperator(
        task_id="validate_minio_csv",
        configure_batch_definition=configure_batch_definition,
        configure_expectations=configure_expectations,
        context_type="ephemeral",
        result_format="SUMMARY",
    )