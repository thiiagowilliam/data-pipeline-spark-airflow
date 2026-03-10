# Troubleshooting

This guide provides solutions to common problems you might encounter while running the project.

## Docker Compose

### Services fail to start

*   **Symptom**: `docker-compose up` fails with errors, or some containers exit unexpectedly.
*   **Solution**:
    1.  Ensure Docker and Docker Compose are installed correctly and that the Docker daemon is running.
    2.  Check for port conflicts. If another service on your machine is using a port required by the project (e.g., 8085, 9001, 5433, 7077), you will need to stop that service or change the port mapping in the `docker-compose.yaml` file.
    3.  Run `docker-compose down -v` to remove all containers, networks, and volumes, and then try `docker-compose up --build` again.

### Permission errors

*   **Symptom**: You get "permission denied" errors when running `docker-compose` commands.
*   **Solution**:
    1.  Add your user to the `docker` group to avoid using `sudo`:
        ```bash
        sudo usermod -aG docker $USER
        ```
        You will need to log out and log back in for this change to take effect.
    2.  Ensure that the files in the project directory have the correct permissions.

## Airflow

### DAGs not appearing in the UI

*   **Symptom**: The `s3_spark_processing_pipeline` DAG is not visible in the Airflow UI.
*   **Solution**:
    1.  Wait a few minutes for the Airflow scheduler to parse the DAG files.
    2.  Check the logs of the `airflow-scheduler` container for any parsing errors:
        ```bash
        docker-compose logs airflow-scheduler
        ```
    3.  Ensure the DAG file (`bronze_pipeline.py`) is in the correct directory (`airflow/dags`).

### Task fails with `AirflowException`

*   **Symptom**: The `run_spark_job` task fails with an `AirflowException` related to `spark-submit`.
*   **Solution**:
    1.  Check the logs of the `spark-master` and `spark-worker` containers for errors:
        ```bash
        docker-compose logs spark-master
        docker-compose logs spark-worker
        ```
    2.  Ensure that the path to the Spark application in the `SparkSubmitOperator` is correct and that the file exists.
    3.  Verify that the Spark cluster is running correctly by accessing the Spark Master UI at `http://localhost:8081`.

## Spark

### `java.lang.ClassNotFoundException`

*   **Symptom**: The Spark job fails with a `ClassNotFoundException`, often related to S3AFileSystem or PostgreSQL driver.
*   **Solution**:
    1.  Ensure that the required packages (`hadoop-aws`, `aws-java-sdk-bundle`, `postgresql`) are correctly specified in the `packages` argument of the `SparkSubmitOperator`.
    2.  Check the internet connection of the Spark containers, as they need to download these packages.

## MinIO

### Cannot connect to MinIO

*   **Symptom**: The data simulator or the Spark job fails to connect to MinIO.
*   **Solution**:
    1.  Verify that the MinIO container is running and accessible at `http://localhost:9001`.
    2.  Check that the MinIO endpoint, access key, and secret key are correctly configured in the `settings.py` file and that they match the values in `docker-compose.yaml`.
