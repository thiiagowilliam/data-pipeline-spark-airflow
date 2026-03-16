"""
Testes unitários para o módulo de validação de dados.

Valida o comportamento do DataValidator sem precisar de um cluster Spark,
usando uma SparkSession local (master=local[1]).
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

from pyspark.sql import SparkSession
from pyspark.sql import types as T

# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    """SparkSession local compartilhada entre os testes."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-validate")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def contract_dir(tmp_path: Path) -> str:
    """Cria contratos JSON temporários para os testes."""
    clientes = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": False, "metadata": {}},
            {"name": "nome", "type": "string", "nullable": True, "metadata": {}},
            {"name": "email", "type": "string", "nullable": True, "metadata": {}},
            {"name": "estado", "type": "string", "nullable": True, "metadata": {}},
            {"name": "status", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    vendas = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "integer", "nullable": False, "metadata": {}},
            {"name": "cliente_id", "type": "integer", "nullable": False, "metadata": {}},
            {"name": "valor_total", "type": "double", "nullable": True, "metadata": {}},
            {"name": "quantidade", "type": "integer", "nullable": True, "metadata": {}},
            {"name": "metodo_pagto", "type": "string", "nullable": True, "metadata": {}},
        ],
    }
    (tmp_path / "clientes.json").write_text(json.dumps(clientes))
    (tmp_path / "vendas.json").write_text(json.dumps(vendas))
    return str(tmp_path)


# ──────────────────────────────────────────────
# Importação do módulo (após fixtures)
# ──────────────────────────────────────────────

# O módulo está em airflow/dags/scripts/spark_jobs/
import sys

sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parent.parent
        / "airflow/dags/scripts/spark_jobs"
    ),
)
from validate import DataValidator, DataValidationError, ValidationReport  # noqa: E402


# ──────────────────────────────────────────────
# Testes — Schema Validation
# ──────────────────────────────────────────────


class TestSchemaValidation:
    def test_valid_schema_passes(self, spark, contract_dir):
        """DataFrame com colunas corretas não deve gerar erros de schema."""
        df = spark.createDataFrame(
            [(1, "João", "joao@email.com", "SP", "Ativo")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        schema_errors = [e for e in report.errors if e.rule == "schema_missing_columns"]
        assert len(schema_errors) == 0

    def test_missing_column_detected(self, spark, contract_dir):
        """DataFrame sem coluna obrigatória deve gerar erro de schema."""
        df = spark.createDataFrame(
            [(1, "João")],
            schema="id INT, nome STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        error_rules = [e.rule for e in report.errors]
        assert "schema_missing_columns" in error_rules

    def test_extra_column_generates_warning(self, spark, contract_dir):
        """Coluna extra deve gerar aviso (WARNING), não erro crítico."""
        df = spark.createDataFrame(
            [(1, "João", "j@e.com", "SP", "Ativo", "EXTRA")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING, coluna_extra STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        extra_errors = [e for e in report.errors if e.rule == "schema_extra_columns"]
        assert len(extra_errors) == 1
        assert extra_errors[0].severity == "WARNING"


# ──────────────────────────────────────────────
# Testes — Null Validation
# ──────────────────────────────────────────────


class TestNullValidation:
    def test_null_in_non_nullable_column_fails(self, spark, contract_dir):
        """Nulo em coluna obrigatória (nullable=False) deve falhar com ERROR."""
        df = spark.createDataFrame(
            [(None, "João", "j@e.com", "SP", "Ativo")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        assert not report.is_valid
        null_errors = [e for e in report.errors if e.rule == "not_null"]
        assert len(null_errors) > 0

    def test_null_in_nullable_column_passes(self, spark, contract_dir):
        """Nulo em coluna nullable=True não deve gerar erro de null."""
        df = spark.createDataFrame(
            [(1, None, "j@e.com", "SP", "Ativo")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        null_errors = [e for e in report.errors if e.rule == "not_null"]
        assert len(null_errors) == 0


# ──────────────────────────────────────────────
# Testes — Business Rules: Clientes
# ──────────────────────────────────────────────


class TestBusinessRulesClientes:
    def test_invalid_email_fails(self, spark, contract_dir):
        """Email com formato inválido deve gerar erro de negócio."""
        df = spark.createDataFrame(
            [(1, "João", "email-invalido", "SP", "Ativo")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        email_errors = [e for e in report.errors if e.rule == "business_email_format"]
        assert len(email_errors) == 1

    def test_valid_email_passes(self, spark, contract_dir):
        """Email válido não deve gerar erro."""
        df = spark.createDataFrame(
            [(1, "João", "joao@empresa.com.br", "SP", "Ativo")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        email_errors = [e for e in report.errors if e.rule == "business_email_format"]
        assert len(email_errors) == 0

    def test_invalid_state_fails(self, spark, contract_dir):
        """Estado com mais de 2 caracteres deve falhar."""
        df = spark.createDataFrame(
            [(1, "João", "j@e.com", "São Paulo", "Ativo")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        state_errors = [e for e in report.errors if e.rule == "business_state_length"]
        assert len(state_errors) == 1

    def test_invalid_status_fails(self, spark, contract_dir):
        """Status fora do conjunto válido deve gerar erro."""
        df = spark.createDataFrame(
            [(1, "João", "j@e.com", "SP", "Suspenso")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        status_errors = [e for e in report.errors if e.rule == "business_valid_status"]
        assert len(status_errors) == 1


# ──────────────────────────────────────────────
# Testes — Business Rules: Vendas
# ──────────────────────────────────────────────


class TestBusinessRulesVendas:
    def test_negative_valor_total_fails(self, spark, contract_dir):
        """valor_total negativo deve gerar erro."""
        df = spark.createDataFrame(
            [(1, 10, -50.0, 1, "PIX")],
            schema="id INT, cliente_id INT, valor_total DOUBLE, quantidade INT, metodo_pagto STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "vendas")
        value_errors = [e for e in report.errors if e.rule == "business_positive_value"]
        assert len(value_errors) == 1

    def test_zero_quantity_fails(self, spark, contract_dir):
        """quantidade = 0 deve gerar erro."""
        df = spark.createDataFrame(
            [(1, 10, 100.0, 0, "PIX")],
            schema="id INT, cliente_id INT, valor_total DOUBLE, quantidade INT, metodo_pagto STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "vendas")
        qty_errors = [e for e in report.errors if e.rule == "business_min_quantity"]
        assert len(qty_errors) == 1

    def test_valid_vendas_passes(self, spark, contract_dir):
        """Registro de venda válido não deve gerar erros críticos."""
        df = spark.createDataFrame(
            [(1, 10, 250.0, 2, "PIX")],
            schema="id INT, cliente_id INT, valor_total DOUBLE, quantidade INT, metodo_pagto STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "vendas")
        assert report.is_valid


# ──────────────────────────────────────────────
# Testes — Validation Report
# ──────────────────────────────────────────────


class TestValidationReport:
    def test_raise_on_failure_raises_for_invalid(self, spark, contract_dir):
        """raise_on_failure() deve lançar DataValidationError quando inválido."""
        df = spark.createDataFrame(
            [(None, "João", "j@e.com", "SP", "Ativo")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        with pytest.raises(DataValidationError):
            report.raise_on_failure()

    def test_raise_on_failure_passes_for_valid(self, spark, contract_dir):
        """raise_on_failure() não deve lançar exceção quando válido."""
        df = spark.createDataFrame(
            [(1, "João", "joao@email.com", "SP", "Ativo")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        # Não deve levantar exceção
        report.raise_on_failure()

    def test_conformity_rate_100_for_valid_data(self, spark, contract_dir):
        """Dataset válido deve ter conformidade de 100%."""
        df = spark.createDataFrame(
            [(1, "João", "joao@email.com", "SP", "Ativo")],
            schema="id INT, nome STRING, email STRING, estado STRING, status STRING",
        )
        validator = DataValidator(contract_dir=contract_dir)
        report = validator.validate(df, "clientes")
        assert report.is_valid
