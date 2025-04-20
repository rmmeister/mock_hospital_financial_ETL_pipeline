# HFDB Data Pipeline

A Python-based ETL pipeline for hospital financial data. It performs:

1. Extraction from Mockaroo or source systems.
2. Preprocessing and transformations.
3. Loading into PostgreSQL.

## Project Structure

- : Contains extract, preprocess, and load scripts.
- : Shared config, logging setup, and utility functions.
- : Airflow DAG definitions.
- : Local storage for raw, processed data, and logs.

## How to Run

1. Install dependencies:
    pip install -r requirements.txt

2. Run the pipeline manually:
    python src/extract.py
    python src/preprocess.py
    python src/load.py

3. Or schedule with Airflow.

## License
MIT
