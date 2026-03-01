# Project Architecture & Design Decisions

This document provides a detailed explanation of the architectural choices and design patterns used in this data pipeline project.

## 1. Core Architecture: The Medallion Lakehouse

We adopted the **Medallion Architecture** to structure our data lake. This pattern provides a clear, logical progression for data as it moves from its raw state to a highly refined, analysis-ready state.

-   **Bronze Layer (Raw Data):**
    -   **Purpose:** To ingest data from source systems with minimal transformation. The goal is to create a durable, timestamped archive of the source data.
    -   **Implementation:** The `ingest_bronze` job reads from the PostgreSQL `sales` table, adds an `ingestion_tms` column, and writes the data as a Delta table to `s3a://bronze/sales`. The `overwrite` mode ensures idempotency for our daily batch runs.

-   **Silver Layer (Cleaned & Conformed Data):**
    -   **Purpose:** To provide a single source of truth for our key business entities. Data here is cleaned, deduplicated, and conformed to a consistent schema.
    -   **Implementation:** The `transform_silver` job reads from the Bronze layer, applies data quality rules (type casting, null handling), and performs deduplication. It then uses a `MERGE` operation to upsert the data into the Silver Delta table, a critical pattern for idempotency.

-   **Gold Layer (Aggregated & Business-Ready Data):**
    -   **Purpose:** To create data models optimized for specific business use cases, such as reporting and analytics.
    -   **Implementation:** The `aggregate_gold` job reads from the Silver layer and produces a new table with business-level aggregations (e.g., total sales by customer per month). This data is written to `s3a://gold/` and is partitioned by `year_month` for efficient querying by BI tools.

**Technology Choice: Delta Lake**
Delta Lake is used as the file format for all layers because it brings ACID transactions, schema enforcement, and time-travel capabilities to our data lake, effectively creating a "Lakehouse."

---

## 2. Key Design Decisions & Trade-offs

### Orchestration: Airflow with KubernetesExecutor

-   **Why Airflow?** It is the de-facto industry standard for batch orchestration, with a massive community and a rich ecosystem of providers.
-   **Decision: KubernetesExecutor vs. Others**
    -   **LocalExecutor:** Simple for getting started but runs all tasks on a single machine, offering no scalability or isolation. Unsuitable for production.
    -   **CeleryExecutor:** A highly scalable option but requires setting up and managing a separate Celery cluster with a message broker (e.g., Redis).
    -   **KubernetesExecutor (Chosen):** The perfect fit for a K8s-native environment. Each Airflow task runs in its own dedicated Kubernetes pod. This provides maximum isolation, allows for task-level resource configuration, and keeps the architecture clean and consistent. While more complex to set up than `LocalExecutor`, it is far simpler to manage than a separate Celery cluster when you're already on Kubernetes.

### Execution Engine: Spark on Kubernetes Operator

This is the most critical design decision for achieving an elastic, cost-effective, and modern data platform.

-   **Trade-off: Spark Standalone (e.g., in Docker Compose)**
    -   *Pros:* Simple to set up for local development.
    -   *Cons:* Creates a **static cluster**. Resources are allocated upfront and are wasted if no jobs are running. It cannot scale automatically based on workload, leading to performance bottlenecks or excessive cost.

-   **Trade-off: Default Spark-on-Kubernetes Scheduler**
    -   *Pros:* Allows Spark to run on Kubernetes without an operator.
    -   *Cons:* It is an **imperative** approach. You must manually run `spark-submit` for every job. It's harder to manage application lifecycles declaratively and integrate with GitOps/IaC principles.

-   **Decision: Spark on Kubernetes Operator (Chosen)**
    -   *Pros:* It provides a **declarative, K8s-native** way to run Spark. We define our job as a `SparkApplication` custom resource, and the operator handles the rest (creating the driver pod, requesting executor pods).
    -   **The Key Benefit: True Elasticity.** By enabling `dynamicAllocation`, the operator can request the exact number of executors a job needs and release them when they are idle. This is the pinnacle of resource efficiency and was a primary goal of this project.

### Data Quality & Idempotency

-   **Lesson Learned: Idempotency is Not Optional.** A production pipeline must be runnable multiple times with the same input, producing the exact same output. Failures are inevitable, and re-running a failed job should not corrupt the data lake.
-   **Implementation (Silver Layer):** The `DeltaTable.merge()` operation is the key. By merging on a unique business key (`id`), we can gracefully handle reruns. If a record already exists, it's updated; if it's new, it's inserted.
-   **Implementation (Gold Layer):** For aggregations, it is often safer and simpler to completely recalculate the data for a given time window. We use `mode("overwrite")` combined with partitioning (`partitionBy("year_month")`) to achieve this. Rerunning the job simply replaces the affected partitions with the newly calculated data.
-   **Data Quality Gates:** The `validate_silver_data` task in the Airflow DAG is designed as a quality gate. In a full implementation, it would use the `contracts/sales_contract.json` to dynamically build a Great Expectations suite and validate the Silver data *before* it's used to build the Gold layer, preventing data quality issues from propagating to end-users.

---

## 3. Observability Strategy

-   **Airflow Logging:** The Airflow Helm chart is configured with `logs.persistence.enabled: true`, which creates a PersistentVolumeClaim. All task logs are written to this volume, ensuring they survive pod restarts.
-   **Spark Job Monitoring:** A **Spark History Server** is deployed via the Spark Operator Helm chart.
    -   Our `SparkApplication` definitions are configured to write event logs to `s3a://spark-logs/`.
    -   The History Server is configured to read from this S3 bucket.
    -   This provides a persistent UI for diagnosing failed Spark jobs, analyzing execution plans, and tuning performance, which is indispensable in a production environment.