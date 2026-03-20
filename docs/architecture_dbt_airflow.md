# Arquitetura Profissional com dbt, Airflow, Terraform e LGPD

Este documento descreve a arquitetura de dados implementada, focada em profissionalismo, governança de dados (LGPD) e orquestração.

## 1. Visão Geral

A arquitetura utiliza uma abordagem moderna para o pipeline de dados:

-   **Terraform:** Gerencia toda a infraestrutura no Google Cloud, incluindo datasets do BigQuery e, crucialmente, a taxonomia de governança de dados (Policy Tags).
-   **dbt:** Ferramenta central para a transformação dos dados. Transforma os dados brutos da camada `bronze` em camadas `silver` (limpa, padronizada) e `gold` (agregada, pronta para o negócio), aplicando as regras de proteção de dados.
-   **Airflow:** Orquestra todo o pipeline, garantindo que os dados sejam processados na ordem correta (bronze -> silver -> gold) e com tratamento de dependências e erros.
-   **BigQuery:** Atua como o Data Warehouse, onde os dados são armazenados e as transformações do dbt são executadas.

## 2. LGPD e Governança de Dados

A conformidade com a LGPD é um pilar desta arquitetura, implementada da seguinte forma:

1.  **Policy Tags via Terraform:** O Terraform cria uma taxonomia no Data Catalog chamada `Governança de Dados (LGPD)` com uma `Policy Tag` chamada `PII - Informação Pessoalmente Identificável`.
2.  **Conexão dbt <> Policy Tags:** O dbt aplica automaticamente essas tags às colunas do BigQuery durante a sua execução (`dbt run`). Isso é feito através da configuração `policy_tags` nos arquivos `schema.yml`.
3.  **Controle de Acesso:** Com as tags aplicadas, é possível criar políticas no IAM do Google Cloud para restringir o acesso a essas colunas. Por exemplo, apenas um grupo de "Administradores de Dados" pode ter permissão para ler colunas marcadas como `PII`.
4.  **Pseudonimização e Mascaramento:** Dados pessoais sensíveis são transformados (hash ou máscara) na transição da camada `bronze` para a `silver`. Isso garante que as camadas analíticas não contenham dados pessoais em formato bruto.

## 3. Estrutura do Projeto dbt

-   **Camadas (`models/`):**
    -   `silver`: Contém modelos que limpam e padronizam os dados. Aqui são aplicadas as regras de LGPD. Os modelos são marcados com a tag `silver`.
    -   `gold`: Contém datamarts e modelos de negócio agregados, prontos para consumo por ferramentas de BI. Os modelos são marcados com a tag `gold`.
-   **Macros (`macros/`):**
    -   `masking.sql`: Contém uma macro reutilizável para mascarar dados, demonstrando uma prática de código limpo e modular.
-   **Configuração (`dbt_project.yml`):**
    -   Define as tags dos modelos (`silver`, `gold`) para permitir a execução seletiva.
    -   Centraliza a definição do caminho da `Policy Tag` em uma variável, facilitando a manutenção.

## 4. Orquestração com Airflow

-   **DAG `dbt_silver_pipeline`**: Executa e testa apenas os modelos da camada `silver` (`dbt run --models tag:silver`).
-   **DAG `dbt_gold_pipeline`**:
    -   Primeiro, aguarda o sucesso da DAG `silver` utilizando um `ExternalTaskSensor`.
    -   Após a confirmação, executa e testa os modelos da camada `gold` (`dbt run --models tag:gold`).
    -   Isso cria um pipeline robusto e modular, onde a camada `gold` só é construída se a `silver` for concluída com sucesso.

## 5. Como Executar o Pipeline Completo

1.  **Configurar Variáveis de Ambiente:** Exporte as seguintes variáveis no seu terminal:
    ```bash
    export GCP_PROJECT_ID="seu-projeto-gcp"
    export GCP_LOCATION="us-central1" # Ou a região desejada
    ```

2.  **Criar a Infraestrutura de Governança:**
    ```bash
    cd infra
    terraform init
    terraform apply
    ```
    Isso criará os datasets no BigQuery e a taxonomia/policy tags de LGPD.

3.  **Instalar Dependências do dbt:**
    ```bash
    cd dbt
    dbt deps
    ```

4.  **Iniciar o Ambiente Airflow:**
    ```bash
    # Na raiz do projeto
    docker-compose up -d
    ```

5.  **Executar o Pipeline no Airflow:**
    -   Acesse a UI do Airflow (normalmente `http://localhost:8080`).
    -   Despause as DAGs `dbt_silver_pipeline` e `dbt_gold_pipeline`.
    -   Para uma execução completa, acione primeiro a DAG que carrega os dados para a camada `bronze` (ex: `bronze_pipeline`).
    -   Após o sucesso da `bronze`, acione a DAG `dbt_silver_pipeline`.
    -   A DAG `dbt_gold_pipeline` será acionada automaticamente após o sucesso da `silver`.
