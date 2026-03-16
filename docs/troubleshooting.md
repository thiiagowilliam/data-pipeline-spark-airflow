# Troubleshooting

Solutions for common issues you might encounter.

## Docker Compose

### Services fail to start

- **Symptom**: `docker compose up` fails or containers exit unexpectedly.
- **Solution**:
  1. Ensure Docker and Docker Compose are installed and the daemon is running.
  2. Check for port conflicts (8085, 9001, 5433, 7077, 18080). Stop conflicting services or change ports in `docker-compose.yaml`.
  3. Run a clean restart:
     ```bash
     make clean   # or: docker compose down -v
     make up      # or: docker compose up --build -d
     ```

### Permission errors

- **Symptom**: "permission denied" errors.
- **Solution**:
  ```bash
  sudo usermod -aG docker $USER
  # Log out and back in
  ```

## Airflow

### DAGs not appearing in the UI

- **Symptom**: The `bronze_data_pipeline` DAG is not visible.
- **Solution**:
  1. Wait a few minutes for the scheduler to parse DAG files.
  2. Check scheduler logs:
     ```bash
     make logs s=airflow-scheduler
     # or: docker compose logs airflow-scheduler
     ```
  3. Verify `bronze_pipeline.py` is in `airflow/dags/`.
  4. Check for Python import errors — the DAG file must parse without errors.

### Validation task fails

- **Symptom**: `validate_data` task fails with `DataValidationError`.
- **Solution**:
  1. Check the Spark job logs for the detailed validation report (schema, types, nulls, business rules).
  2. Verify that the CSV data matches the contracts in `contracts/`.
  3. Common issues:
     - Missing columns in the CSV
     - Wrong data types (e.g., string where integer expected)
     - Invalid email format in `clientes`
     - `valor_total <= 0` or `quantidade < 1` in `vendas`

### Spark job fails with `AirflowException`

- **Symptom**: `ingest_to_bronze` task fails.
- **Solution**:
  1. Check Spark cluster logs:
     ```bash
     make logs s=spark-master
     make logs s=spark-worker
     ```
  2. Verify application path exists: `/opt/airflow/dags/scripts/spark_jobs/bronze_ingest.py`
  3. Check Spark Master UI at http://localhost:8081

## Spark

### `java.lang.ClassNotFoundException`

- **Symptom**: Missing S3AFileSystem or PostgreSQL driver.
- **Solution**: Verify the `packages` parameter in the DAG's `SparkSubmitOperator` includes:
  - `org.apache.hadoop:hadoop-aws:3.3.4`
  - `com.amazonaws:aws-java-sdk-bundle:1.12.540`
  - `org.postgresql:postgresql:42.7.2` (ingest job only)

### SparkSession config issues

- **Symptom**: Cannot connect to MinIO from Spark.
- **Solution**: Check `spark_config.py` — all S3A settings are centralized there. Verify env vars:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `MINIO_ENDPOINT`

## MinIO

### Cannot connect to MinIO

- **Symptom**: Simulator or Spark job fails to connect.
- **Solution**:
  1. Check MinIO is running: http://localhost:9001
  2. Verify credentials match `docker-compose.yaml`:
     - User: `minioadmin`
     - Password: `minioadmin`
  3. From inside Docker, use endpoint `http://minio:9000` (not `localhost`)
