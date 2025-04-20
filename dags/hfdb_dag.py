from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.extract import run as extract_run
from include.preprocess import run as preprocess_run
from include.load import run as load_run

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
with DAG(
    "data_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",  # correct
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_run,
    )

    preprocess_task = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_run,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_run,
    )

    # Task dependencies
    extract_task >> preprocess_task >> load_task


# from airflow import DAG
# from airflow.operators.empty import EmptyOperator
# from datetime import datetime

# with DAG(
#     dag_id="simple_test_dag",
#     start_date=datetime(2024, 4, 10),
#     schedule_interval="@hourly",
#     catchup=False
# ) as dag:

#     start = EmptyOperator(task_id="start")
