import os
import io
import json
import logging
import time
from datetime import datetime
from glob import glob
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

CLIENT_ROWS = int(os.getenv("CLIENT_ROWS", "55000"))
SALES_ROWS = int(os.getenv("SALES_ROWS", "70000"))
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "300"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataGenerator:
    def __init__(self, fake_instance: Faker):
        self.fake = fake_instance
        # Mapeamento de nomes de campos para funções do Faker
        self.faker_map = {
            "nome": self.fake.name,
            "email": self.fake.email,
            "telefone": self.fake.phone_number,
            "cidade": self.fake.city,
            "estado": self.fake.state_abbr,
            "data_cadastro": lambda: self.fake.date_between(start_date="-2y", end_date="today"),
            "data_venda": lambda: datetime.now().date(),
            "status": lambda: np.random.choice(["Ativo", "Inativo", "Pendente"]),
            "metodo_pagto": lambda: np.random.choice(["Cartão de Crédito", "Boleto", "PIX", "Dinheiro"]),
        }

    def generate_from_schema(self, schema_path: str, num_rows: int) -> pd.DataFrame:
        with open(schema_path, 'r') as f:
            contract = json.load(f)
        
        fields = contract.get("fields", [])
        data = {}

        for field in fields:
            name = field["name"]
            dtype = field["type"]

            if name == "id":
                data[name] = np.arange(1, num_rows + 1)
            elif "id" in name:
                data[name] = np.random.randint(1, 1000, size=num_rows)
            elif name in self.faker_map:
                data[name] = [self.faker_map[name]() for _ in range(num_rows)]
            else:
                if dtype == "integer":
                    data[name] = np.random.randint(1, 100, size=num_rows)
                elif dtype == "double":
                    data[name] = np.round(np.random.uniform(10.0, 5000.0, size=num_rows), 2)
                else:
                    data[name] = [self.fake.word() for _ in range(num_rows)]
        
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
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.contracts_dir = os.path.normpath(os.path.join(script_dir, "../contracts"))

    def run(self):
        logging.info("Iniciando simulador baseado em contratos...")
        self.uploader.create_bucket()

        while True:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_files = glob(os.path.join(self.contracts_dir, "*.json"))

            for file_path in json_files:
                file_name = os.path.basename(file_path)
                contract_name = file_name.replace(".json", "")
                rows = SALES_ROWS if "venda" in contract_name else CLIENT_ROWS
                df = self.generator.generate_from_schema(file_path, rows)
                self.uploader.upload_to_minio(df, contract_name, f"{contract_name}_{timestamp}.csv")

            logging.info(f"Aguardando {INTERVAL_SECONDS / 60:.0f} minutos para próxima execução...")
            time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    fake_instance = Faker("pt_BR")
    data_generator = DataGenerator(fake_instance)
    minio_uploader = MinioUploader(MINIO_URL, ACCESS_KEY, SECRET_KEY, BUCKET_NAME)
    simulation = Simulation(data_generator, minio_uploader)
    simulation.run()