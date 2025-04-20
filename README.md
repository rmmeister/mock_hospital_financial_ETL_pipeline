# HFDB Airflow Data Pipeline

A Python-based ETL pipeline for hospital financial data. It performs:

1. Extraction from Mockaroo or source systems.
2. Preprocessing and transformations.
3. Loading into PostgreSQL.

This project uses Astronomer CLI and Apache Airflow to run an ETL pipeline for hospital financial data.

## Getting Started

1. Install the Astro CLI:
    brew install astronomer/tap/astro

2. Start the Airflow environment:
    astro dev start

3. Visit Airflow UI at:
    http://localhost:8080

4. Stop the environment:
    astro dev stop

## Dependencies

Python packages are listed in `requirements.txt`. Astro installs them automatically inside the Airflow container.

## Folder Structure

- `dags/`: Your Airflow DAGs.
- `include/`: Shared Python code.
- `tests/`: Pytest-based test suite.

## Project Structure

- : Contains extract, preprocess, and load scripts.
- : Shared config, logging setup, and utility functions.
- : Airflow DAG definitions.
- : Local storage for raw, processed data, and logs.

## License
MIT
