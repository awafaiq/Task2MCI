from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mmds_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 2, # Menaikkan retry untuk mengantisipasi endpoint API timeout
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'orders_realtime_pipeline',
    default_args=default_args,
    schedule_interval='*/15 * * * *', # Dijalankan setiap 15 Menit (Micro-batching)
    catchup=False,
    max_active_runs=1,
    description='Pipeline API Orders -> Spark Transformation -> ClickHouse Load'
) as dag:

    # Task 1: Extract dari IP API
    task_extract_api = BashOperator(
        task_id='fetch_orders_data',
        bash_command='python /opt/airflow/dags/scripts/fetch_orders_api.py'
    )

    # Task 2: Transform dengan Spark & Load ke ClickHouse
    task_transform_load = BashOperator(
        task_id='process_and_load_clickhouse',
        bash_command='python /opt/airflow/dags/scripts/process_orders_sparks.py'
    )

    # Definisi Dependensi (Arrow)
    task_extract_api >> task_transform_load