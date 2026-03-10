# Data Model and Schemas

This document describes the data models and schemas used in the pipeline.

## Raw Data

The raw data is generated in CSV format and has the following columns:

### `clientes.csv`

*   `id`: Unique identifier for the customer.
*   `nome`: Customer's name.
*   `email`: Customer's email address.
*   `data_cadastro`: Registration date.
*   `telefone`: Phone number.

### `vendas.csv`

*   `id`: Unique identifier for the sale.
*   `id_cliente`: Foreign key referencing the customer.
*   `produto`: Product name.
*   `quantidade`: Quantity of the product sold.
*   `preco_unitario`: Unit price of the product.
*   `data_venda`: Date of the sale.

## Bronze Layer (Parquet)

The data in the bronze layer is stored in Parquet format. The schemas are the same as the raw data, but with data types inferred by Spark. This layer serves as the single source of truth for the transformed data.

## PostgreSQL Data Warehouse

The final data is loaded into a PostgreSQL database with the following tables:

### `clientes`

```sql
CREATE TABLE clientes (
    id INTEGER PRIMARY KEY,
    nome VARCHAR(255),
    email VARCHAR(255),
    data_cadastro TIMESTAMP,
    telefone VARCHAR(20)
);
```

### `vendas`

```sql
CREATE TABLE vendas (
    id INTEGER PRIMARY KEY,
    id_cliente INTEGER REFERENCES clientes(id),
    produto VARCHAR(255),
    quantidade INTEGER,
    preco_unitario DECIMAL(10, 2),
    data_venda TIMESTAMP
);
```

## Data Contracts

The `contracts/` directory contains JSON files that formally define the schemas. These contracts can be used for schema validation and evolution.

*   `clientes.json`: Defines the schema for the `clientes` dataset.
*   `vendas.json`: Defines the schema for the `vendas` dataset.
*   `sales_contract.json`: Contains data quality rules for the `vendas` dataset, intended for use with a tool like Great Expectations. For example, it can define that `id` should be unique and not null.
