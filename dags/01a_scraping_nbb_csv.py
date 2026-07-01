import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow')
from utils.consult_nbb import process_nbb_csv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 100,
}

with DAG(
    '01a_scraping_nbb_csv',
    default_args=default_args,
    description='Extraction prioritaire des CSV NBB',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nbb', 'csv', 'high-priority'],
) as dag:

    scraping_csv_task = PythonOperator(
        task_id='run_nbb_csv_scraper',
        python_callable=process_nbb_csv,
    )