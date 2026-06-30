import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append('/opt/airflow')
from scripts.strapor_notaire import process_notaire

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    '02_scraping_notaire',
    default_args=default_args,
    description='Extraction des actes notariés via session Playwright et Tor',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['notaire', 'scraping', 'pdf', 'playwright', 'tor'],
) as dag:

    scraping_notaire_task = PythonOperator(
        task_id='run_notaire_scraper',
        python_callable=process_notaire,
    )