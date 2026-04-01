# Análise Cirúrgica da Pipeline de Engenharia de Dados com Pyspark

Este documento fornece uma análise técnica detalhada da pipeline de dados, dissecando cada componente, configuração e script para oferecer uma compreensão profunda de sua arquitetura e funcionamento.

## Sumário

- [1. Visão Geral da Arquitetura](#1-visão-geral-da-arquitetura)
- [2. Análise do Ambiente Conteinerizado](#2-análise-do-ambiente-conteinerizado)
    - [2.1. Docker Compose (`docker-compose.yaml`)](#21-docker-compose-docker-composeyaml)
    - [2.2. Imagem do Airflow (`Dockerfile.airflow`)](#22-imagem-do-airflow-dockerfileairflow)
    - [2.3. Imagem do Spark (`Dockerfile.spark`)](#23-imagem-do-spark-dockerfilespark)
- [3. Orquestração com Airflow](#3-orquestração-com-airflow)
    - [3.1. O DAG `etl_clientes_medallion`](#31-o-dag-etl_clientes_medallion)
    - [3.2. Gerenciamento de Conexões e Segredos](#32-gerenciamento-de-conexões-e-segredos)
- [4. Jobs de Processamento com Spark (Data Plane)](#4-jobs-de-processamento-com-spark-data-plane)
    - [4.1. Configuração da Sessão Spark (`spark_config.py`)](#41-configuração-da-sessão-spark-spark_configpy)
    - [4.2. Job 1: Raw para Bronze (`raw_to_bronze_bucket_ingest.py`)](#42-job-1-raw-para-bronze-raw_to_bronze_bucket_ingestpy)
    - [4.3. Job 2: Bronze para Silver (`bronze_to_silver_bucket_ingest.py`)](#43-job-2-bronze-para-silver-bronze_to_silver_bucket_ingestpy)
    - [4.4. Job 3: Silver para Gold (BigQuery) (`silver_bucket_to_bq.py`)](#44-job-3-silver-para-gold-bigquery-silver_bucket_to_bqpy)
- [5. Contratos de Dados](#5-contratos-de-dados)
- [6. Simulação de Dados](#6-simulação-de-dados)
- [7. Infraestrutura como Código (Terraform)](#7-infraestrutura-como-código-terraform)

---

## 1. Visão Geral da Arquitetura

A solução implementa uma pipeline ETL seguindo a arquitetura **Medallion**, orquestrada pelo Apache Airflow e executada por um cluster Apache Spark. Os dados fluem de um bucket "raw" em um serviço de objeto S3 (MinIO), são processados em camadas "bronze" e "silver" usando o formato Delta Lake, e finalmente carregados em uma camada "gold" no Google BigQuery.

```mermaid
graph TD
    subgraph "Ambiente Local (Docker)"
        subgraph "Geração"
            SIM["Data Simulator"]
        end
        subgraph "Orquestração (Control Plane)"
            AFW["Airflow (Scheduler, Webserver, etc.)"]
        end
        subgraph "Processamento (Data Plane)"
            SPK["Cluster Spark (Master, Worker)"]
        end
        subgraph "Data Lake"
            MIO["MinIO (S3)"]
        end
    end

    subgraph "Cloud (GCP)"
        BQ["Google BigQuery"]
    end
    
    SIM -- "CSV" --> MIO_RAW[/"raw"/]
    AFW -- "SparkSubmitOperator" --> SPK
    SPK -- "Job 1: Ingest" --> MIO_RAW
    SPK -- "Job 1: Ingest" --> MIO_BRONZE[/"bronze" (Delta)/]
    SPK -- "Job 2: Quality" --> MIO_BRONZE
    SPK -- "Job 2: Quality" --> MIO_SILVER[/"silver" (Delta)/]
    SPK -- "Job 2: Quality" --> MIO_QUARANTINE[/"quarantine" (Delta)/]
    SPK -- "Job 3: Load" --> MIO_SILVER
    SPK -- "Job 3: Load" --> BQ
    
    style MIO_RAW fill:#FFAB91
    style MIO_BRONZE fill:#FFCC80
    style MIO_SILVER fill:#C5E1A5
    style MIO_QUARANTINE fill:#EF9A9A
    style BQ fill:#90CAF9
```

## 2. Análise do Ambiente Conteinerizado

### 2.1. Docker Compose (`docker-compose.yaml`)

O `docker-compose.yaml` define e interliga todos os serviços da aplicação.

-   **YAML Anchors (`&airflow-common-env`, `&airflow-common`)**: São utilizados para evitar repetição de código, definindo configurações comuns para os serviços Airflow.
-   **Serviços Principais**:
    -   `postgres`: Persiste os metadados do Airflow em um volume (`postgres_data`).
    -   `minio`: Simula o Amazon S3 para o Data Lake, com dados persistidos no volume `minio_data`. Expõe a porta `9000` para a API e `9001` para o console web.
    -   `spark-master`/`spark-worker`: Formam o cluster de processamento. Os workers se registram no master via `spark://spark-master:7077`.
    -   `spark-history-server`: Permite a análise post-mortem de jobs Spark, lendo logs do volume compartilhado `events/spark-events`.
-   **Inicialização do Airflow**:
    -   O serviço `airflow-init` é um container temporário que executa `airflow db migrate` e `airflow users create`. Ele garante que o banco de dados esteja pronto e um usuário admin seja criado antes que os outros serviços do Airflow iniciem.
    -   Os serviços `airflow-scheduler`, `airflow-webserver` e `airflow-dag-processor` dependem do `airflow-init`.

### 2.2. Imagem do Airflow (`Dockerfile.airflow`)

-   **Base**: `apache/airflow:3.1.8`.
-   **Dependências**:
    -   Instala o `default-jdk-headless`, um pré-requisito para a comunicação do Airflow com o Spark.
    -   Adiciona os providers Airflow essenciais: `apache-spark`, `amazon` (para interagir com o MinIO) e `google` (para o BigQuery).
    -   Instala bibliotecas Python como `pandas` e `dbt-bigquery` para tarefas auxiliares.

### 2.3. Imagem do Spark (`Dockerfile.spark`)

-   **Base**: `apache/spark:4.1.1`.
-   **Conectividade S3**:
    -   Faz o download dos JARs `hadoop-aws` e `aws-sdk` diretamente do Maven. Estes são **essenciais** para que o Spark possa usar o filesystem `s3a://` para se comunicar com o MinIO.
-   **Configuração Padrão (`spark-defaults.conf`)**:
    -   Um arquivo `spark-defaults.conf` é criado na imagem com configurações para apontar o `s3a` para o endpoint do MinIO (`http://minio:9000`), desabilitar SSL e usar credenciais simples. Isso simplifica os jobs, que não precisam repetir essas configurações.

## 3. Orquestração com Airflow

### 3.1. O DAG `etl_clientes_medallion`

Localizado em `airflow/dags/etl_pipeline.py`, este DAG é o cérebro da pipeline.

-   **Agendamento**: `schedule="* * 1 * *"` (executa todo dia primeiro de cada mês). `catchup=False` previne execuções retroativas.
-   **Operadores**:
    -   `SparkSubmitOperator`: É o principal operador utilizado, responsável por submeter os scripts PySpark para o cluster.
    -   `@task (TaskFlow API)`: Usado na tarefa `move_to_archive` para uma implementação Python pura que utiliza o `S3Hook` para mover arquivos processados.
-   **Fluxo de Tarefas**: O fluxo é definido explicitamente, garantindo a ordem correta de execução: a ingestão para bronze ocorre primeiro, seguida em paralelo pelo arquivamento dos dados brutos e pela sequência de qualidade (bronze -> silver) e carga (silver -> bq).

### 3.2. Gerenciamento de Conexões e Segredos

A pipeline evita credenciais hardcoded de forma elegante:

-   **Conexão AWS (`aws_default`)**: As credenciais para o MinIO são armazenadas em uma conexão Airflow do tipo "Amazon Web Services". O `SparkSubmitOperator` as injeta no job Spark através do campo `conf`, usando templates Jinja:
    ```python
    "spark.hadoop.fs.s3a.access.key": "{{ conn.aws_default.extra_dejson.aws_access_key }}"
    ```
-   **Conexão Google Cloud (`google_cloud_default`)**: A chave da Service Account do GCP é armazenada no campo "Keyfile JSON" da conexão. O DAG a passa para o job de carga do BigQuery como uma variável de ambiente, convertendo-a para JSON:
    ```python
    env_vars={"GCP_KEY_JSON": "{{ conn.google_cloud_default.extra | tojson }}"}
    ```

## 4. Jobs de Processamento com Spark (Data Plane)

### 4.1. Configuração da Sessão Spark (`spark_config.py`)

Este script centraliza a criação da `SparkSession`, garantindo consistência. Ele habilita o suporte a Delta Lake e configura o acesso ao S3A.

### 4.2. Job 1: Raw para Bronze (`raw_to_bronze_bucket_ingest.py`)

-   **Estratégia de Leitura/Escrita**: Utiliza o `readStream` e `writeStream` com o gatilho `availableNow=True`. Esta é uma técnica moderna que processa todos os dados disponíveis em uma única "tacada" (micro-batch), mas mantendo os benefícios do Structured Streaming, como o checkpointing para idempotência.
-   **Lógica**:
    1.  Lê arquivos CSV do diretório `raw/`.
    2.  Adiciona a coluna `dt_ingest` com a data de execução para particionamento.
    3.  Escreve os dados no formato Delta na camada `bronze/`. O checkpoint garante que, se o job falhar e for re-executado, os mesmos dados não serão escritos duas vezes.

### 4.3. Job 2: Bronze para Silver (`bronze_to_silver_bucket_ingest.py`)

Este é o job mais complexo, contendo a lógica de negócio e qualidade de dados.

-   **Desenvolvimento Orientado a Contrato**: O job lê um "contrato" em JSON (`contracts/clientes.json`) que define as regras de qualidade.
-   **Validação de Dados (`validate` method)**:
    1.  Para cada campo do contrato, constrói uma expressão booleana no Spark.
    2.  **Qualidade de Tipo**: `col(field.name).cast(field.spark_type).isNull()` identifica valores que não podem ser convertidos para o tipo esperado.
    3.  **Qualidade de Formato**: `~col(field.name).rlike(field.regex)` usa regex do contrato para validar formatos (ex: CPF, email).
    4.  **Qualidade de Conteúdo**: `~col(field.name).isin(field.in_set)` verifica se o valor pertence a um conjunto pré-definido.
    5.  **Unicidade**: Usa uma Window Function (`row_number().over(...) > 1`) para marcar registros duplicados com base nas chaves únicas do contrato.
    6.  As condições de erro são combinadas com um `OR` (`reduce(lambda x, y: x | y, ...)`), criando uma única coluna booleana `erros`.
-   **Separação e Quarentena**: O DataFrame é dividido em `df_validos` e `df_erros`. Os errados são salvos na pasta `quarantine/`.
-   **Proteção de PII**: Campos marcados com `"pii": true` no contrato são anonimizados usando hash `sha2(..., 256)`.
-   **Operação `MERGE` (Upsert)**:
    -   Para os dados válidos, uma operação `MERGE` é executada na tabela Delta da camada Silver.
    -   A condição de merge (`ON ...`) é construída dinamicamente com base nos campos marcados com `"unique": true` no contrato.
    -   `whenMatchedUpdateAll()`: Atualiza o registro se a chave já existir.
    -   `whenNotMatchedInsertAll()`: Insere o registro se a chave for nova.

### 4.4. Job 3: Silver para Gold (BigQuery) (`silver_bucket_to_bq.py`)

-   **Estratégia de Carga**: Também utiliza `readStream` / `writeStream` com `availableNow=True` e `foreachBatch`, lendo da camada Silver.
-   **Conector BigQuery**:
    -   Usa o formato `"bigquery"`.
    -   O método de escrita é `"direct"`, que é mais performático para grandes volumes.
-   **Autenticação Segura**:
    -   O job lê a string JSON da Service Account da variável de ambiente `GCP_KEY_JSON`.
    -   **Importante**: Para passar a credencial ao conector Spark, ela é codificada em **Base64** e passada via `.option("credentials", b64_creds)`. Este é o mecanismo padrão para autenticação por chave quando não se está em um ambiente GCP nativo.
-   **Metadados de Auditoria**: Adiciona as colunas `_bq_run_id` e `_bq_loaded_at` para rastreabilidade.

## 5. Contratos de Dados

Os arquivos JSON em `contracts/` são o pilar da validação de dados. Eles desacoplam as regras de negócio do código Spark. Um contrato define:

-   `fields`: Esquema (nome, tipo).
-   `regex`: Expressões regulares para validação de formato.
-   `in_set`: Listas de valores permitidos.
-   `unique`: Se o campo é uma chave de negócio (para `MERGE`).
-   `pii`: Se o campo deve ser anonimizado.

## 6. Simulação de Dados

O script `simulator/data_simulator.py`:
- Utiliza a biblioteca `Faker` para gerar dados realistas em português.
- É também "orientado a contrato", lendo os mesmos arquivos JSON para saber quais colunas gerar.
- Usa `boto3` para fazer o upload dos dados em formato CSV para o bucket `raw/` no MinIO, iniciando o ciclo da pipeline.
- Opera em um loop infinito, gerando novos arquivos em intervalos configuráveis.

## 7. Infraestrutura como Código (Terraform)

O diretório `infra/` contém o código Terraform para provisionar os recursos na nuvem.
- **`main.tf`**: Define o módulo `bigquery`.
- **`bigquery/main.tf`**: Contém o recurso `google_bigquery_dataset`, que cria o dataset no BigQuery onde as tabelas da camada Gold serão armazenadas. Isso garante que a infraestrutura seja gerenciável e versionada.