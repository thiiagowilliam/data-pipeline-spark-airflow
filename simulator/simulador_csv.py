import pandas as pd
import numpy as np
from faker import Faker
import boto3
from botocore.client import Config
import time
from datetime import datetime
import io
import os
import logging

# --- CONFIGURACOES ---
MINIO_URL = "http://localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "admin123"
BUCKET_NAME = "datalake"

# --- CONFIG LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

fake = Faker('pt_BR')

# --- CLIENTE S3 ---
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

def generate_clients(num_rows=550000):
    logging.info(f"Gerando {num_rows} clientes...")
    data = {
        "id": np.arange(1, num_rows + 1),
        "nome": [fake.name() for _ in range(num_rows)],
        "email": [fake.email() for _ in range(num_rows)],
        "telefone": [fake.phone_number() for _ in range(num_rows)],
        "cidade": [fake.city() for _ in range(num_rows)],
        "estado": [fake.state_abbr() for _ in range(num_rows)],
        "data_cadastro": [fake.date_between(start_date='-2y', end_date='today') for _ in range(num_rows)],
        "status": np.random.choice(['Ativo', 'Inativo', 'Pendente'], num_rows)
    }
    return pd.DataFrame(data)

def generate_sales(num_rows=700000):
    logging.info(f"Gerando {num_rows} vendas...")
    data = {
        "id": np.arange(1, num_rows + 1),
        "cliente_id": np.random.randint(1, 500000, size=num_rows),
        "produto_id": np.random.randint(1, 1000, size=num_rows),
        "data_venda": [datetime.now().date()] * num_rows,
        "valor_total": np.round(np.random.uniform(10.0, 5000.0, size=num_rows), 2),
        "quantidade": np.random.randint(1, 10, size=num_rows),
        "metodo_pagto": np.random.choice(['Cartão de Crédito', 'Boleto', 'PIX', 'Dinheiro'], num_rows)
    }
    return pd.DataFrame(data)

def upload_to_minio(df, filename):
    """Converte DF para CSV e envia para o bucket"""
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    file_size_mb = len(csv_buffer.getvalue()) / (1024 * 1024)
    logging.info(f"Enviando {filename} ({file_size_mb:.2f} MB) para o MinIO...")
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=f"raw/{filename}",
        Body=csv_buffer.getvalue()
    )
    logging.info(f"Sucesso: {filename} enviado.")

def run_simulation():
    """Loop principal que roda a cada 5 minutos"""
    while True:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df_clients = generate_clients()
        upload_to_minio(df_clients, f"clientes_{timestamp}.csv")
        df_sales = generate_sales()
        upload_to_minio(df_sales, f"vendas_{timestamp}.csv")

        logging.info("Aguardando 5 minutos para o próximo envio...")
        time.sleep(300)

if __name__ == "__main__":
    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        logging.info(f"Bucket '{BUCKET_NAME}' verificado/criado com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao criar o bucket: {e}")

    run_simulation()