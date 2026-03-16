import os
import io
import logging
import time
from datetime import datetime
from typing import Dict, Any, List

import numpy as np
import pandas as pd
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

MINIO_URL = os.getenv("MINIO_URL", "http://localhost:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "datalake")

CLIENT_ROWS = int(os.getenv("CLIENT_ROWS", "550000"))
SALES_ROWS = int(os.getenv("SALES_ROWS", "700000"))
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "300"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataGenerator:
    def __init__(self, fake_instance: Faker):
        self.fake = fake_instance

    def generate_clients(self, num_rows: int = CLIENT_ROWS) -> pd.DataFrame:
        logging.info(f"Gerando {num_rows} clientes...")
        data: Dict[str, Any] = {
            "id": np.arange(1, num_rows + 1),
            "nome": [self.fake.name() for _ in range(num_rows)],
            "email": [self.fake.email() for _ in range(num_rows)],
            "telefone": [self.fake.phone_number() for _ in range(num_rows)],
            "cidade": [self.fake.city() for _ in range(num_rows)],
            "estado": [self.fake.state_abbr() for _ in range(num_rows)],
            "data_cadastro": [self.fake.date_between(start_date="-2y", end_date="today") for _ in range(num_rows)],
            "status": np.random.choice(["Ativo", "Inativo", "Pendente"], num_rows),
        }
        return pd.DataFrame(data)

    def generate_sales(self, num_rows: int = SALES_ROWS) -> pd.DataFrame:
        logging.info(f"Gerando {num_rows} vendas...")
        data: Dict[str, Any] = {
            "id": np.arange(1, num_rows + 1),
            "cliente_id": np.random.randint(1, CLIENT_ROWS, size=num_rows),
            "produto_id": np.random.randint(1, 1000, size=num_rows),
            "data_venda": [datetime.now().date()] * num_rows,
            "valor_total": np.round(np.random.uniform(10.0, 5000.0, size=num_rows), 2),
            "quantidade": np.random.randint(1, 10, size=num_rows),
            "metodo_pagto": np.random.choice(["Cartão de Crédito", "Boleto", "PIX", "Dinheiro"], num_rows),
        }
        return pd.DataFrame(data)

class MinioUploader:
    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
        )

    def create_bucket(self):
        try:
            self.s3_client.create_bucket(Bucket=self.bucket_name)
            logging.info(f"Bucket '{self.bucket_name}' criado.")
        except self.s3_client.exceptions.BucketAlreadyOwnedByYou:
            logging.info(f"Bucket '{self.bucket_name}' já existe.")
        except Exception as e:
            logging.error(f"Erro ao criar bucket: {e}")

    def upload_to_minio(self, df: pd.DataFrame, folder: str, filename: str):
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        body = csv_buffer.getvalue()

        file_size_mb = len(body) / (1024 * 1024)
        key = f"raw/{folder}/{filename}"

        logging.info(f"Enviando {filename} ({file_size_mb:.2f} MB) para {key}")
        self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=body)
        logging.info(f"Upload concluído: {filename}")


class Simulation:
    def __init__(self, generator: DataGenerator, uploader: MinioUploader):
        self.generator = generator
        self.uploader = uploader

    def run(self):
        logging.info("Iniciando simulador de ingestão de dados...")
        self.uploader.create_bucket()

        while True:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            df_clients = self.generator.generate_clients()
            self.uploader.upload_to_minio(df_clients, "clientes", f"clientes_{timestamp}.csv")

            df_sales = self.generator.generate_sales()
            self.uploader.upload_to_minio(df_sales, "vendas", f"vendas_{timestamp}.csv")

            logging.info(f"Aguardando {INTERVAL_SECONDS / 60:.0f} minutos para próxima execução...")
            time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    fake_instance = Faker("pt_BR")
    data_generator = DataGenerator(fake_instance)
    minio_uploader = MinioUploader(MINIO_URL, ACCESS_KEY, SECRET_KEY, BUCKET_NAME)
    simulation = Simulation(data_generator, minio_uploader)
    simulation.run()
