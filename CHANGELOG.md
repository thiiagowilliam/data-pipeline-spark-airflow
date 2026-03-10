# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Created `CHANGELOG.md` to track changes.
- Added comprehensive `README.md` with project overview, architecture, and setup instructions.
- Created `docs` directory with detailed documentation for architecture, data model, and troubleshooting.
- Added `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md`.
- Added `requirements.txt` to manage Python dependencies.
- Added `.pre-commit-config.yaml` for code quality checks.
- Added unit tests for the data simulator.
- Integrated Great Expectations for data validation.

### Changed
- Refactored `simulator/simulador_csv.py` to `simulator/data_simulator.py` to use classes and environment variables.
- Refactored `bronze_pipeline.py` DAG to use Airflow connections and variables.
- Refactored `bronze_client_ingest.py` Spark job to be more modular and use environment variables.
- Switched from Parquet to Delta Lake for the bronze layer.
- Updated `.gitignore` with more patterns.
- Updated `Dockerfile.airflow` and `Dockerfile.spark` to install dependencies from `requirements.txt`.
- Updated CI workflow to use `pre-commit` and `pytest`.

### Removed
- Removed `helmfile` related content from `README.md`.
- Removed `airflow/dags/scripts/connection.py` and `airflow/dags/scripts/settings.py`.
