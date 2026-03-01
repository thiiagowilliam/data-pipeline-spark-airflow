# Production-Grade Medallion Data Lakehouse on Kubernetes

Pipeline de dados end-to-end com arquitetura Medallion (Bronze, Silver, Gold) rodando nativamente no Kubernetes com elasticidade real via Apache Spark Kubernetes Operator, PySpark, Delta Lake e Airflow KubernetesExecutor.

[![PySpark 3.5.1](https://img.shields.io/badge/PySpark-3.5.1-yellow.svg)](https://spark.apache.org/)
[![Airflow 2.10.2](https://img.shields.io/badge/Airflow-2.10.2-blue.svg)](https://airflow.apache.org/)
[![Delta Lake 3.2.0](https://img.shields.io/badge/Delta%20Lake-3.2.0-green.svg)](https://delta.io/)
[![Spark Operator](https://img.shields.io/badge/Spark%20Operator-latest-orange.svg)](https://github.com/kubeflow/spark-operator)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-grey.svg?logo=kubernetes)](https://kubernetes.io/)
[![k3d](https://img.shields.io/badge/k3d-latest-brightgreen.svg)](https://k3d.io/)
[![Great Expectations](https://img.shields.io/badge/Great%20Expectations-latest-purple.svg)](https://greatexpectations.io/)

---

## Arquitetura: Medallion Lakehouse Elástico

A pipeline segue o padrão Medallion para refinar dados progressivamente, com orquestração via Airflow e processamento distribuído e elástico via Spark on Kubernetes.

```mermaid
graph TD
    subgraph "Fontes de Dados"
        A[PostgreSQL Database]
    end

    subgraph "Kubernetes Cluster (k3d)"
        subgraph "Data Lake (MinIO)"
            B[<font color=brown>Bronze Layer</font> - Raw Delta]
            C[<font color=silver>Silver Layer</font> - Cleaned/Validated Delta]
            D[<font color=gold>Gold Layer</font> - Aggregated Delta]
        end

        subgraph "Orquestração"
             E[Airflow (KubernetesExecutor)] -- Triggers CRD --> F{Spark Operator};
        end
        
        subgraph "Processamento Elástico (On-Demand)"
            F -- Creates --> G[Spark Driver Pod];
            G -- "dynamicAllocation" --> H[Executor Pod 1..N];
        end
    end

    A -- "1. Ingest (JDBC)" --> G;
    G -- "Write Raw" --> B;
    B -- "Read Raw" --> G;
    G -- "Clean, Deduplicate, Validate & MERGE" --> C;
    C -- "Read Cleaned" --> G;
    G -- "Aggregate & Write" --> D;

    style B fill:#CD7F32,stroke:#333,stroke-width:2px;
    style C fill:#C0C0C0,stroke:#333,stroke-width:2px;
    style D fill:#FFD700,stroke:#333,stroke-width:2px;
```

---

## Por que Spark Kubernetes Operator?

Em setups tradicionais (Spark Standalone), os workers são **fixos**: você aloca recursos e paga por eles mesmo quando ociosos. O **Spark Kubernetes Operator** muda o jogo:

1.  **Zero Cluster Persistente:** Jobs são submetidos como CRDs (`SparkApplication`). O operator cria e destrói pods de forma on-demand.
2.  **`dynamicAllocation` Real:** Configurado com `minExecutors` e `maxExecutors`, o Spark adiciona/remove executors *durante a execução* baseado na carga (shuffles, etc.), otimizando o uso de recursos.
3.  **Isolamento e Custo Zero:** Cada job roda em seu próprio conjunto de pods, liberando 100% dos recursos ao terminar.

> *[Placeholder para GIF: `kubectl get pods -n spark-jobs -w` mostrando pods de executors escalando de 1 para 8 e depois terminando.]*

---

## Como Rodar em 4 Minutos

**Pré-requisitos:** Docker, `k3d`, `helm`, `kubectl`.

1.  **Clone o Repositório**
    ```bash
    git clone https://github.com/thiiagowilliam/data-pipeline-spark-airflow.git
    cd data-pipeline-spark-airflow
    ```

2.  **Crie o Cluster Kubernetes Local**
    ```bash
    k3d cluster create --config infra/kubernetes/k3d-datalake.yaml
    ```

3.  **Construa e Importe as Imagens Customizadas**
    ```bash
    # Build
    docker build -t pyspark-lakehouse-spark:latest -f infra/docker/Dockerfile.spark .
    docker build -t pyspark-lakehouse-airflow:latest -f infra/docker/Dockerfile.airflow .
    
    # Import para o cluster k3d (mais rápido que um registry local)
    k3d image import pyspark-lakehouse-spark:latest pyspark-lakehouse-airflow:latest --cluster datalake
    ```

4.  **Deploy da Infraestrutura via Helm**
    ```bash
    bash infra/kubernetes/setup-k8s.sh
    ```
    Aguarde os pods nos namespaces `airflow`, `minio`, e `postgres` estarem no estado `Running`. Use `kubectl get pods -A -w`.

5.  **Acesse, Ative e Execute a Pipeline**
    -   **Airflow UI:** `http://localhost:8080` (user: `airflow`, pass: `airflow`)
    -   **MinIO Console:** `http://localhost:9001` (user: `minio`, pass: `minio123`)

    No Airflow UI, ative a DAG `medallion_sales_pipeline` e dispare uma execução manual.

> *[Placeholder para GIF: Navegação no Airflow UI, ativando e executando a DAG com sucesso.]*

> *[Placeholder para GIF: Navegação no MinIO Console mostrando as pastas `bronze`, `silver` e `gold` sendo populadas com arquivos Parquet/Delta.]*

---

## Decisões Técnicas e Lições Aprendidas

-   **Idempotência com Delta `MERGE`:** A operação de `MERGE` (upsert) na camada Silver é crucial para garantir que re-execuções da pipeline não gerem duplicatas.
-   **KubernetesExecutor:** Garante que cada task do Airflow rode em seu próprio pod, proporcionando isolamento total e permitindo que a submissão dos jobs Spark seja nativa do K8s.
-   **k3d com `image import`:** Para desenvolvimento local, `k3d image import` é significativamente mais simples e rápido do que configurar e usar um registry local.

## Roadmap Futuro

-   [ ] Implementar CI/CD com GitHub Actions (lint, test, build).
-   [ ] Aprofundar validações com Great Expectations, gerando Data Docs.
-   [ ] Adicionar uma pipeline de streaming com Kafka e Spark Structured Streaming.
-   [ ] Integrar com Trino/Presto para consultas SQL federadas no Data Lakehouse.

---
_Este projeto foi lapidado seguindo um prompt detalhado com o Code Agent do Google Gemini._