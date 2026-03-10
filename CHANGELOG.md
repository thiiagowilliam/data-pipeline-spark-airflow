# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Data Validation Layer**: Professional `DataValidator` class with schema, type, null, and business rule validation
- **Validation Spark Job** (`validate_data.py`): Standalone job for pre-ingest data quality checks
- **Validation Reports**: Structured reports with conformity rates and detailed error breakdowns
- **Centralized Spark Config** (`spark_config.py`): Shared SparkSession factory for all Spark jobs
- **Makefile**: DevOps automation with `make up`, `make test`, `make lint`, `make simulate`
- **DAG Parse Verification**: CI/CD step to validate all Python files parse correctly
- Created `CHANGELOG.md` to track changes
- Added comprehensive `README.md` with tech stack badges, architecture diagram, project structure
- Created `docs/` directory with architecture, data model, and troubleshooting docs
- Added `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md`
- Added `.pre-commit-config.yaml` for code quality checks
- Added unit tests for the data simulator
- Integrated Great Expectations for data validation

### Changed
- **Refactored DAG**: Extracted Spark config constants (DRY), removed unused imports, added `doc_md` to all tasks
- **Refactored Spark Jobs**: Both `bronze_ingest.py` and `validate_data.py` now use `spark_config.py`
- **Improved CI/CD**: Split into 3 jobs (lint, test, verify-dag), added pytest-cov, updated GitHub Actions
- **Updated Documentation**: Fixed stale references, synchronized data model with actual schemas
- Refactored `simulator/data_simulator.py` to use classes and environment variables
- Switched from Parquet to Delta Lake for the bronze layer
- Updated `Dockerfile.airflow` and `Dockerfile.spark` to install dependencies from `requirements.txt`

### Removed
- Removed orphan `Dockerfile` (not referenced by docker-compose)
- Removed `airflow/dags/scripts/connection.py` and `airflow/dags/scripts/settings.py`
