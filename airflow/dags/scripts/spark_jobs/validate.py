from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

log = logging.getLogger(__name__)

_SPARK_TYPE_MAP: dict[str, type[T.DataType]] = {
    "id": T.IntegerType,
    "long": T.LongType,
    "float": T.FloatType,
    "double": T.DoubleType,
    "string": T.StringType,
    "boolean": T.BooleanType,
    "date": T.DateType,
    "timestamp": T.TimestampType,
}

_COMPATIBLE_TYPES: dict[type[T.DataType], tuple[type[T.DataType], ...]] = {
    T.FloatType: (T.DoubleType,),
    T.DoubleType: (T.FloatType,),
    T.DateType: (T.StringType,),
    T.TimestampType: (T.StringType,),
}  

@dataclass
class ValidationError:
    rule: str
    column: str
    message: str
    severity: str = "ERROR"
    failed_count: int = 0

@dataclass
class ValidationReport:
    dataset: str
    total_rows: int = 0
    total_errors: int = 0
    errors: list[ValidationError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        critical = [e for e in self.errors if e.severity == "ERROR"]
        return len(critical) == 0

    @property
    def conformity_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        max_possible = self.total_rows * max(len(self.errors), 1)
        return round((1 - self.total_errors / max_possible) * 100, 2)

    def add_error(self, error: ValidationError) -> None:
        self.errors.append(error)
        self.total_errors += error.failed_count

    def raise_on_failure(self) -> None:
        if not self.is_valid:
            summary = self._build_summary()
            raise DataValidationError(
                f"Validação falhou para dataset '{self.dataset}'.\n{summary}"
            )

    def log_summary(self) -> None:
        summary = self._build_summary()
        level = logging.INFO if self.is_valid else logging.ERROR
        log.log(level, "\n%s", summary)

    def _build_summary(self) -> str:
        border = "=" * 60
        status = "✅ APROVADO" if self.is_valid else "❌ REPROVADO"
        lines = [
            border,
            f"  RELATÓRIO DE VALIDAÇÃO — {self.dataset.upper()}",
            border,
            f"  Status          : {status}",
            f"  Total de linhas : {self.total_rows:,}",
            f"  Total de erros  : {self.total_errors:,}",
            f"  Conformidade    : {self.conformity_rate}%",
            f"  Regras violadas : {len(self.errors)}",
            border,
        ]
        if self.errors:
            lines.append("  DETALHES DAS VIOLAÇÕES:")
            lines.append("-" * 60)
            for i, err in enumerate(self.errors, 1):
                lines.append(
                    f"  {i}. [{err.severity}] {err.rule} | "
                    f"Coluna: {err.column} | Falhas: {err.failed_count:,}"
                )
                lines.append(f"     → {err.message}")
            lines.append(border)
        return "\n".join(lines)


class DataValidationError(Exception):
    """Exceção levantada quando a validação de dados falha."""

class DataValidator:
    def __init__(self, contract_dir: str = "/opt/airflow/dags/contracts"):
        self.contract_dir = Path(contract_dir)
        self._contracts: dict[str, dict[str, Any]] = {}

    def _load_contract(self, dataset: str) -> dict[str, Any]:
        if dataset in self._contracts:
            return self._contracts[dataset]

        contract_path = self.contract_dir / f"{dataset}.json"
        if not contract_path.exists():
            raise FileNotFoundError(
                f"Contrato não encontrado: {contract_path}"
            )

        with open(contract_path, "r", encoding="utf-8") as f:
            contract = json.load(f)

        self._contracts[dataset] = contract
        log.info("Contrato carregado: %s", contract_path.name)
        return contract

    def validate(self, df: DataFrame, dataset: str) -> ValidationReport:
        
        report = ValidationReport(dataset=dataset, total_rows=df.count())
        contract = self._load_contract(dataset)

        log.info(
            "Iniciando validação do dataset '%s' (%s linhas)",
            dataset,
            f"{report.total_rows:,}",
        )

        self._validate_schema(df, contract, report)

        self._validate_types(df, contract, report)

        self._validate_nulls(df, contract, report)

        self._validate_business_rules(df, dataset, report)

        report.log_summary()
        return report

    def _validate_schema(
        self, df: DataFrame, contract: dict, report: ValidationReport
    ) -> None:
        expected_columns = [f["name"] for f in contract.get("fields", [])]
        actual_columns = df.columns

        missing = set(expected_columns) - set(actual_columns)
        extra = set(actual_columns) - set(expected_columns)

        if missing:
            report.add_error(
                ValidationError(
                    rule="schema_missing_columns",
                    column=", ".join(sorted(missing)),
                    message=f"Colunas ausentes no DataFrame: {sorted(missing)}",
                    severity="ERROR",
                    failed_count=len(missing),
                )
            )

        if extra:
            report.add_error(
                ValidationError(
                    rule="schema_extra_columns",
                    column=", ".join(sorted(extra)),
                    message=f"Colunas inesperadas no DataFrame: {sorted(extra)}",
                    severity="WARNING",
                    failed_count=len(extra),
                )
            )

    def _validate_types(
        self, df: DataFrame, contract: dict, report: ValidationReport
    ) -> None:
        
        for field_def in contract.get("fields", []):
            col_name = field_def["name"]
            expected_type = field_def["type"].lower()

            if col_name not in df.columns:
                continue 

            expected_spark_type = _SPARK_TYPE_MAP.get(expected_type)
            if expected_spark_type is None:
                continue
            actual_type = type(df.schema[col_name].dataType)
            if actual_type == expected_spark_type:
                continue

            compatible = _COMPATIBLE_TYPES.get(expected_spark_type, ())
            if actual_type in compatible:
                log.debug(
                    "Coluna '%s': tipo compatível aceito (%s ≡ %s)",
                    col_name,
                    expected_spark_type.__name__,
                    actual_type.__name__,
                )
                continue

            report.add_error(
                ValidationError(
                    rule="type_mismatch",
                    column=col_name,
                    message=(
                        f"Tipo esperado: {expected_type}, "
                        f"tipo encontrado: {df.schema[col_name].dataType.simpleString()}"
                    ),
                    severity="ERROR",
                    failed_count=1,
                )
            )

    def _validate_nulls(self, df: DataFrame, contract: dict, report: ValidationReport) -> None:
        for field_def in contract.get("fields", []):
            col_name = field_def["name"]
            nullable = field_def.get("nullable", True)

            if nullable or col_name not in df.columns:
                continue

            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count > 0:
                report.add_error(
                    ValidationError(
                        rule="not_null",
                        column=col_name,
                        message=f"{null_count:,} valores nulos encontrados",
                        severity="ERROR",
                        failed_count=null_count,
                    )
                )

    def _validate_business_rules(
        self, df: DataFrame, dataset: str, report: ValidationReport
    ) -> None:
        rules_map = {
            "clientes": self._rules_clientes,
            "vendas": self._rules_vendas,
        }
        rule_fn = rules_map.get(dataset)
        if rule_fn:
            rule_fn(df, report)

    def _rules_clientes(self, df: DataFrame, report: ValidationReport) -> None:
        null_ids = df.filter(F.col("id").isNull()).count()
        if null_ids > 0:
            report.add_error(
                ValidationError(
                    rule="business_not_null_id",
                    column="id",
                    message=f"{null_ids:,} registros sem ID",
                    severity="ERROR",
                    failed_count=null_ids,
                )
            )

        if "email" in df.columns:
            email_regex = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
            invalid_emails = df.filter(
                ~F.col("email").rlike(email_regex) & F.col("email").isNotNull()
            ).count()
            if invalid_emails > 0:
                report.add_error(
                    ValidationError(
                        rule="business_email_format",
                        column="email",
                        message=f"{invalid_emails:,} e-mails com formato inválido",
                        severity="ERROR",
                        failed_count=invalid_emails,
                    )
                )

        if "estado" in df.columns:
            invalid_states = df.filter(
                (F.length(F.col("estado")) != 2) & F.col("estado").isNotNull()
            ).count()
            if invalid_states > 0:
                report.add_error(
                    ValidationError(
                        rule="business_state_length",
                        column="estado",
                        message=f"{invalid_states:,} estados com formato inválido (esperado 2 caracteres)",
                        severity="ERROR",
                        failed_count=invalid_states,
                    )
                )

        if "status" in df.columns:
            valid_statuses = ["Ativo", "Inativo", "Pendente", "ativo", "inativo", "pendente"]
            invalid_status = df.filter(
                ~F.col("status").isin(valid_statuses) & F.col("status").isNotNull()
            ).count()
            if invalid_status > 0:
                report.add_error(
                    ValidationError(
                        rule="business_valid_status",
                        column="status",
                        message=f"{invalid_status:,} registros com status inválido (válidos: {valid_statuses})",
                        severity="ERROR",
                        failed_count=invalid_status,
                    )
                )

    def _rules_vendas(self, df: DataFrame, report: ValidationReport) -> None:
        for col_name in ("id", "cliente_id"):
            if col_name in df.columns:
                null_count = df.filter(F.col(col_name).isNull()).count()
                if null_count > 0:
                    report.add_error(
                        ValidationError(
                            rule=f"business_not_null_{col_name}",
                            column=col_name,
                            message=f"{null_count:,} registros sem {col_name}",
                            severity="ERROR",
                            failed_count=null_count,
                        )
                    )

        if "valor_total" in df.columns:
            invalid_values = df.filter(
                (F.col("valor_total") <= 0) & F.col("valor_total").isNotNull()
            ).count()
            if invalid_values > 0:
                report.add_error(
                    ValidationError(
                        rule="business_positive_value",
                        column="valor_total",
                        message=f"{invalid_values:,} vendas com valor_total <= 0",
                        severity="ERROR",
                        failed_count=invalid_values,
                    )
                )

        if "quantidade" in df.columns:
            invalid_qty = df.filter(
                (F.col("quantidade") < 1) & F.col("quantidade").isNotNull()
            ).count()
            if invalid_qty > 0:
                report.add_error(
                    ValidationError(
                        rule="business_min_quantity",
                        column="quantidade",
                        message=f"{invalid_qty:,} vendas com quantidade < 1",
                        severity="ERROR",
                        failed_count=invalid_qty,
                    )
                )

        if "metodo_pagto" in df.columns:
            valid_methods = [
                "Cartão de Crédito", "Boleto", "PIX", "Dinheiro",
                "cartão de crédito", "boleto", "pix", "dinheiro",
            ]
            invalid_methods = df.filter(
                ~F.col("metodo_pagto").isin(valid_methods)
                & F.col("metodo_pagto").isNotNull()
            ).count()
            if invalid_methods > 0:
                report.add_error(
                    ValidationError(
                        rule="business_valid_payment_method",
                        column="metodo_pagto",
                        message=f"{invalid_methods:,} vendas com método de pagamento inválido",
                        severity="WARNING",
                        failed_count=invalid_methods,
                    )
                )

        if "id" in df.columns:
            total = df.count()
            distinct = df.select("id").distinct().count()
            duplicates = total - distinct
            if duplicates > 0:
                report.add_error(
                    ValidationError(
                        rule="business_unique_id",
                        column="id",
                        message=f"{duplicates:,} IDs duplicados encontrados",
                        severity="WARNING",
                        failed_count=duplicates,
                    )
                )

def validate_bronze(df: DataFrame, dataset: str) -> bool:
    """
    Função de compatibilidade com código existente.
    Mantida para não quebrar o bronze_ingest.py.
    """
    contract_dir = os.getenv(
        "CONTRACT_DIR", "/opt/airflow/dags/contracts"
    )
    validator = DataValidator(contract_dir=contract_dir)
    report = validator.validate(df, dataset)
    report.raise_on_failure()
    return True