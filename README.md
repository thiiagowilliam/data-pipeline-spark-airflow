# PySpark Basic ETL Project

This project is a basic ETL (Extract, Transform, Load) pipeline using PySpark and Airflow to process data from a MinIO Datalake.

## Overview

The pipeline is designed to:

1.  **Extract**: Read CSV files from the `raw` layer of a MinIO bucket.
2.  **Transform**: 
    - Validate the schema of the data against a predefined contract.
    - Remove duplicates.
    - Handle missing values.
3.  **Load**: Save the processed data to the `silver` layer of the MinIO bucket in Parquet format.

## Project Structure

- `airflow/dags`: Contains the Airflow DAG that defines the pipeline.
- `contracts`: Contains the JSON contract files that define the expected schema for the data.
- `simulator`: Contains a Python script to generate and upload data to the MinIO bucket.
- `spark_jobs`: Contains the PySpark jobs that are executed by the pipeline.

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/pyspark-basic.git
    cd pyspark-basic
    ```

2.  **Build and run the services:**
    ```bash
    docker-compose up --build
    ```

### Running the Pipeline

1.  **Access the Airflow UI:**
    - Open your browser and go to `http://localhost:8080`.
    - The default username and password are `airflow`.

2.  **Enable the DAG:**
    - In the Airflow UI, you will see a DAG named `s3_spark_processing_pipeline`.
    - Enable the DAG by clicking the toggle button on the left.

3.  **Run the data simulator:**
    - Open a new terminal and run the following command:
    ```bash
    docker-compose exec airflow-worker bash -c "python /opt/airflow/dags/simulator/simulador_csv.py"
    ```

4.  **Monitor the pipeline:**
    - In the Airflow UI, you can monitor the progress of the pipeline.
    - The pipeline will be triggered automatically when new data is available in the `raw` layer of the MinIO bucket.# data-pipeline-spark-airflow
