# Pyspark Basic - Pipeline de Engenharia de Dados

Este projeto apresenta uma pipeline de engenharia de dados completa e conteinerizada, construída com um stack de ferramentas open-source. O objetivo é demonstrar um fluxo de ETL robusto e escalável, desde a ingestão de dados brutos até a sua disponibilização em um ambiente analítico, seguindo boas práticas de desenvolvimento e arquitetura de dados.

## Visão Geral Técnica

A pipeline orquestra a ingestão de dados simulados de `clientes` e `vendas`, aplicando um processo de validação baseado em contratos de dados, transformando e carregando os dados em uma camada **Bronze** utilizando Delta Lake e, finalmente, disponibilizando-os em um Data Warehouse no Google BigQuery.

A arquitetura foi projetada para ser executada localmente, garantindo um ciclo de desenvolvimento e testes ágil, ao mesmo tempo que utiliza tecnologias prontas para ambientes de produção.

## Índice

- [Arquitetura e Componentes](#arquitetura-e-componentes)
- [Fluxo de Dados](#fluxo-de-dados)
- [Modelo de Dados e Contratos](#modelo-de-dados-e-contratos)
- [Pré-requisitos](#pré-requisitos)
- [Setup do Ambiente](#setup-do-ambiente)
- [Execução e Operação](#execução-e-operação)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Testes, Validação e Qualidade de Código](#testes-validação-e-qualidade-de-código)
- [Extensibilidade e Evolução](#extensibilidade-e-evolução)
- [Monitoramento e Debugging](#monitoramento-e-debugging)

## Arquitetura e Componentes

A arquitetura desacoplada permite a evolução independente de cada componente.

```mermaid
graph LR
    subgraph Geração de Dados
        SIM["Data Simulator<br/>(Faker + Pandas)"]
    end

    subgraph Storage & Datalake
        RAW["MinIO (S3) — Bucket: raw"]
        BRONZE["MinIO (S3) — Bucket: bronze<br/>(Formato Delta Lake)"]
        ARCHIVE["MinIO (S3) — Bucket: archives"]
    end
    
    subgraph Data Warehouse
        BQ["Google BigQuery"]
    end

    subgraph Orquestração (Control Plane)
        AF["Airflow<br/>(CeleryExecutor)"]
        REDIS["Redis<br/>(Message Broker)"]
        PG["PostgreSQL<br/>(Airflow Metastore)"]
    end

    subgraph Processamento (Data Plane)
        SPARK_MASTER["Spark Master"]
        SPARK_WORKER["Spark Worker"]
        SPARK_UI["Spark History Server"]
    end

    SIM -->|Upload de CSVs| RAW
    RAW -- S3KeySensor --> AF
    AF -- SparkSubmitOperator --> SPARK_MASTER
    SPARK_MASTER -- Executa Job --> SPARK_WORKER
    SPARK_WORKER -- Leitura --> RAW
    SPARK_WORKER -- Escrita Delta --> BRONZE
    SPARK_WORKER -- Escrita BQ --> BQ
    SPARK_WORKER -- Leitura/Cópia --> ARCHIVE
    AF -.-> PG
    AF -.-> REDIS
    SPARK_MASTER -.-> SPARK_UI
```

### Detalhamento dos Componentes

-   **Orquestração (Apache Airflow)**: Utiliza o `CeleryExecutor` para distribuição de tarefas, permitindo a execução paralela e escalável dos processos. O `PostgreSQL` atua como metastore, e o `Redis` como message broker.
-   **Processamento (Apache Spark)**: Cluster Standalone com 1 Master e 1 Worker. As aplicações Spark são submetidas pelo Airflow e executam a lógica de validação, transformação e carga. A configuração do Spark (`SPARK_CONF`) é centralizada no DAG e injetada nos jobs, garantindo consistência.
-   **Storage (MinIO)**: Atua como um Object Storage compatível com a API do Amazon S3. É utilizado para armazenar os dados nas camadas `raw`, `bronze` (Delta Lake) e `archives`.
-   **Data Warehouse (Google BigQuery)**: Armazena os dados processados e estruturados, prontos para consumo analítico.
-   **Infraestrutura como Código (Terraform)**: Gerencia a criação do dataset e das tabelas no BigQuery, garantindo que a infraestrutura seja versionada e reprodutível.
-   **Contêineres (Docker)**: Todos os serviços são isolados em contêineres Docker, gerenciados pelo `docker-compose.yaml`, o que simplifica o setup e elimina inconsistências entre ambientes.

## Fluxo de Dados

O fluxo é orquestrado pela DAG `data_bronze` e segue as seguintes etapas:

1.  **File Sensing (`wait_for_s3_file`)**: A DAG inicia com um `S3KeySensor` que monitora a chegada de novos arquivos CSV no bucket `raw/clientes/`. O sensor utiliza `wildcard_match` para detectar qualquer arquivo que corresponda ao padrão. A tarefa opera no modo `reschedule`, liberando o worker do Airflow enquanto aguarda.

2.  **Ingestão para Camada Bronze (`raw_to_bronze_delta_clientes_ingestion`)**:
    -   **Trigger**: Após a detecção de um novo arquivo, o `SparkSubmitOperator` é acionado.
    -   **Job**: `bronze_bucket.py`.
    -   **Parâmetros**: `[dataset, source_path, destination_path, processing_date]`.
    -   **Lógica**:
        -   Lê os arquivos CSV do diretório `raw/clientes`.
        -   Converte os dados para o formato Delta Lake.
        -   Salva a tabela Delta no path `bronze/clientes`. O modo de escrita é `append`, permitindo a ingestão incremental.

3.  **Validação da Camada Bronze (`validate_bronze_clientes`)**:
    -   **Job**: `bronze_validation.py`.
    -   **Lógica**:
        -   Lê a tabela Delta recém-criada na camada bronze.
        -   Aplica um conjunto de regras de validação (ex: checagem de nulos, formato de e-mail, etc.).
        -   *Nota: Em um cenário de produção, esta tarefa poderia gerar um relatório de qualidade e, em caso de falha, acionar um alerta ou mover os dados para uma área de quarentena.*

4.  **Carga no BigQuery (`raw_to_bronze_bigquery_clients_ingestion`)**:
    -   **Job**: `bronze_bigquery.py`.
    -   **Lógica**:
        -   Lê os dados validados da tabela Delta na camada bronze.
        -   Utiliza o conector do Spark para BigQuery para carregar os dados.
        -   A escrita é feita em modo `append`. As credenciais são obtidas a partir de um arquivo JSON de service account montado no contêiner.

5.  **Arquivamento dos Dados Brutos (`move_raw_to_archive`)**:
    -   **Lógica**: Uma `@task` do Airflow utiliza o `S3Hook` para listar os arquivos processados na pasta `raw/`, copiá-los para a pasta `archives/{data_atual}/` e, em seguida, removê-los da origem. Isso evita o reprocessamento e mantém o diretório `raw` limpo.

## Modelo de Dados e Contratos

-   **Raw (CSV)**: Esquemas definidos em `docs/data-model.md`.
-   **Bronze (Delta Lake)**: Mantém o esquema dos dados brutos, mas com os tipos de dados otimizados pelo Spark e a garantia de ACID e versionamento do Delta Lake.
-   **Contratos de Dados**: Arquivos JSON em `contracts/` definem o esquema esperado para cada dataset. Atualmente, a validação é feita via código Spark, mas o projeto está preparado para a integração com ferramentas como Great Expectations.

## Pré-requisitos

-   Docker e Docker Compose
-   GNU Make
-   SDK do Google Cloud (`gcloud`) autenticado.
-   Um projeto no GCP com a API do BigQuery habilitada e uma Service Account com as seguintes roles:
    -   `BigQuery Data Editor`
    -   `BigQuery Job User`

## Setup do Ambiente

1.  **Clonar o Repositório**:
    ```bash
    git clone https://github.com/your-username/pyspark-basic.git
    cd pyspark-basic
    ```

2.  **Credenciais do GCP**:
    a. Baixe o JSON da sua Service Account do GCP.
    b. Salve-o como `airflow/dags/credentials/gcp_key.json`.
    c. Autentique-se no `gcloud` localmente para que o Terraform possa operar:
    ```bash
    gcloud auth login
    gcloud auth application-default login
    ```

3.  **Provisionar Infraestrutura (Terraform)**:
    O Terraform gerenciará o dataset e as tabelas no BigQuery.
    ```bash
    cd infra
    terraform init
    terraform plan  # Revise as mudanças
    terraform apply # Aplique para criar os recursos
    ```

4.  **Iniciar os Serviços**:
    Use o `Makefile` para construir e iniciar todos os contêineres:
    ```bash
    make up
    ```
    Este comando sobe todos os serviços definidos no `docker-compose.yaml` em modo detached.

5.  **Acessar as UIs**:
    -   **Airflow**: `http://localhost:8085` (usuário: `admin`, senha: `admin`)
    -   **MinIO**: `http://localhost:9001` (usuário: `minioadmin`, senha: `minioadmin`)
    -   **Spark Master**: `http://localhost:8081`
    -   **Spark History Server**: `http://localhost:18080`

6.  **Ativar a DAG**:
    No painel do Airflow, ative (un-pause) a DAG `data_bronze`.

## Execução e Operação

-   **Geração de Dados**: Para iniciar o fluxo, gere novos dados. O simulador envia os arquivos diretamente para o MinIO.
    ```bash
    make simulate
    ```
-   **Trigger da Pipeline**: A chegada dos arquivos no MinIO acionará a DAG automaticamente. O progresso pode ser acompanhado na UI do Airflow.
-   **Verificação**:
    -   **MinIO**: Verifique os buckets `bronze` e `archives`.
    -   **BigQuery**: Consulte as tabelas para confirmar a chegada dos novos dados.
    -   **Spark UI**: Analise os jobs concluídos para detalhes de performance.

### Comandos `make`

-   `make up`: Inicia o ambiente.
-   `make down`: Para os contêineres.
-   `make restart`: Reinicia o ambiente.
-   `make logs s=<serviço>`: Exibe logs de um serviço específico (ex: `make logs s=spark-worker`).
-   `make test`: Executa os testes unitários com `pytest`.
-   `make lint`: Roda os linters configurados no `pre-commit`.
-   `make clean`: Para todos os serviços e **remove os volumes de dados**, limpando o ambiente completamente.

## Estrutura do Projeto

```
.
├── airflow/
│   ├── dags/
│   │   ├── bronze_pipeline.py    # Definição da DAG principal
│   │   └── scripts/
│   │       └── spark_jobs/       # Scripts Python com a lógica dos jobs Spark
├── contracts/                    # Contratos de dados em JSON
├── docs/                         # Documentação funcional e de arquitetura
├── infra/                        # Código Terraform para o BigQuery
├── jars/                         # Dependências (JARs) para o Spark (ex: conectores S3 e Delta)
├── simulator/
│   └── data_simulator.py         # Script para geração de dados sintéticos
├── tests/                        # Testes unitários e de integração
├── .pre-commit-config.yaml       # Configuração do pre-commit para linters
├── docker-compose.yaml           # Orquestração dos contêineres
├── Dockerfile.airflow            # Imagem customizada para o Airflow
├── Makefile                      # Atalhos para comandos comuns
└── requirements.txt              # Dependências Python
```

## Testes, Validação e Qualidade de Código

-   **Testes Unitários**: O diretório `tests/` contém testes para a lógica do simulador e validadores. Utilize `make test`.
-   **Qualidade de Código**: O `pre-commit` está configurado para rodar `black`, `isort` e `flake8` automaticamente antes de cada commit. A execução manual é feita com `make lint`.
-   **Validação de Dados**: A validação de esquema e regras de negócio é executada dentro dos jobs Spark, com a lógica definida em `bronze_validation.py`.

## Extensibilidade e Evolução

-   **Novas Fontes de Dados**: Para adicionar um novo dataset, crie um novo script Spark em `spark_jobs/`, adicione uma nova sequência de tarefas na DAG `bronze_pipeline.py` (ou crie uma nova DAG) e defina o contrato de dados correspondente.
-   **Camadas Silver e Gold**: Para evoluir a pipeline, crie novas DAGs que consumam dados da camada Bronze. Estas DAGs orquestrariam jobs Spark para aplicar lógicas de negócio mais complexas, como agregações, enriquecimento e modelagem, gerando as tabelas das camadas Silver e Gold.
-   **Great Expectations**: O projeto pode ser estendido para usar o Great Expectations de forma mais integrada. Isso envolveria a criação de um "Validation Operator" customizado no Airflow ou a execução de checkpoints do GE como um passo nos jobs Spark.

## Monitoramento e Debugging

-   **Logs do Airflow**: A UI do Airflow permite visualizar os logs de cada tarefa, o que é o primeiro passo para debugar falhas na orquestração. Logs detalhados também estão disponíveis nos contêineres (`make logs s=...`).
-   **Spark UI**: A UI do Spark Master (`http://localhost:8081`) mostra o status do cluster e dos jobs em execução. Para análises de performance e erros em jobs já concluídos, utilize o **Spark History Server** (`http://localhost:18080`). Ele oferece uma visão detalhada das stages, tasks e do plano de execução de cada job.
-   **Debugging de Jobs Spark**: Em caso de falha em um job Spark, os logs de erro completos estarão visíveis tanto na saída da tarefa no Airflow quanto no `stderr` do job na UI do Spark.
-   **Acesso Direto aos Serviços**: Para um debugging mais a fundo, é possível acessar os contêineres diretamente: `docker exec -it <container_name> bash`.