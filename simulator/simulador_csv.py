import pandas as pd
import numpy as np
from faker import Faker
import boto3
from botocore.client import Config
import time
from datetime import datetime
import io
import logging

# ===============================
# CONFIGURAÇÕES
# ===============================

MINIO_URL = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "datalake"

CLIENT_ROWS = 550000
SALES_ROWS = 700000

INTERVAL_SECONDS = 300  # 5 minutos

# ===============================
# LOGGING
# ===============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ===============================
# FAKER
# ===============================

fake = Faker("pt_BR")

# ===============================
# CLIENTE S3 / MINIO
# ===============================

s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

# ===============================
# GERADOR DE CLIENTES
# ===============================

def generate_clients(num_rows=CLIENT_ROWS):

    logging.info(f"Gerando {num_rows} clientes...")

    data = {
        "id": np.arange(1, num_rows + 1),
        "nome": [fake.name() for _ in range(num_rows)],
        "email": [fake.email() for _ in range(num_rows)],
        "telefone": [fake.phone_number() for _ in range(num_rows)],
        "cidade": [fake.city() for _ in range(num_rows)],
        "estado": [fake.state_abbr() for _ in range(num_rows)],
        "data_cadastro": [
            fake.date_between(start_date="-2y", end_date="today")
            for _ in range(num_rows)
        ],
        "status": np.random.choice(
            ["Ativo", "Inativo", "Pendente"],
            num_rows
        )
    }

    return pd.DataFrame(data)

# ===============================
# GERADOR DE VENDAS
# ===============================

def generate_sales(num_rows=SALES_ROWS):

    logging.info(f"Gerando {num_rows} vendas...")

    data = {
        "id": np.arange(1, num_rows + 1),
        "cliente_id": np.random.randint(1, CLIENT_ROWS, size=num_rows),
        "produto_id": np.random.randint(1, 1000, size=num_rows),
        "data_venda": [datetime.now().date()] * num_rows,
        "valor_total": np.round(
            np.random.uniform(10.0, 5000.0, size=num_rows), 2
        ),
        "quantidade": np.random.randint(1, 10, size=num_rows),
        "metodo_pagto": np.random.choice(
            ["Cartão de Crédito", "Boleto", "PIX", "Dinheiro"],
            num_rows
        )
    }

    return pd.DataFrame(data)

# ===============================
# UPLOAD PARA MINIO
# ===============================

def upload_to_minio(df, folder, filename):

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    file_size_mb = len(csv_buffer.getvalue()) / (1024 * 1024)

    logging.info(
        f"Enviando {filename} ({file_size_mb:.2f} MB) para raw/{folder}/"
    )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"raw/{folder}/{filename}",
        Body=csv_buffer.getvalue()
    )

    logging.info(f"Upload concluído: {filename}")

# ===============================
# CRIAÇÃO DO BUCKET
# ===============================

def create_bucket():

    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        logging.info(f"Bucket '{BUCKET_NAME}' criado.")
    except Exception:
        logging.info(f"Bucket '{BUCKET_NAME}' já existe.")

# ===============================
# LOOP PRINCIPAL
# ===============================

def run_simulation():

    logging.info("Iniciando simulador de ingestão de dados...")

    while True:

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Clientes
        df_clients = generate_clients()
        upload_to_minio(
            df_clients,
            "clientes",
            f"clientes_{timestamp}.csv"
        )

        # Vendas
        df_sales = generate_sales()
        upload_to_minio(
            df_sales,
            "vendas",
            f"vendas_{timestamp}.csv"
        )

        logging.info(
            f"Aguardando {INTERVAL_SECONDS/60:.0f} minutos para próxima execução..."
        )

        time.sleep(INTERVAL_SECONDS)

# ===============================
# MAIN
# ===============================

if __name__ == "__main__":

    create_bucket()
    run_simulation()