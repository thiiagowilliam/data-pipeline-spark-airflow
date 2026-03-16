CREATE TABLE IF NOT EXISTS clientes (
    id INTEGER PRIMARY KEY,
    nome VARCHAR(255),
    email VARCHAR(255),
    telefone VARCHAR(50),
    cidade VARCHAR(100),
    estado VARCHAR(50),
    data_cadastro DATE,
    status VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS vendas (
    id INTEGER PRIMARY KEY,
    cliente_id INTEGER,
    produto_id INTEGER,
    data_venda DATE,
    valor_total FLOAT,
    quantidade INTEGER,
    metodo_pagto VARCHAR(50),
    FOREIGN KEY (cliente_id) REFERENCES clientes(id)
);

CREATE TABLE IF NOT EXISTS airflow_file_metadata (
    id SERIAL PRIMARY KEY,
    file_key VARCHAR(500) UNIQUE NOT NULL,
    process_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL
);