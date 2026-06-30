import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, '/opt/airflow')
from utils.consult_nbb import process_nbb_pdf

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 1,
}

with DAG(
    '01b_scraping_nbb_pdf',
    default_args=default_args,
    description='Extraction secondaire des PDF NBB',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nbb', 'pdf', 'low-priority'],
) as dag:

    scraping_pdf_task = PythonOperator(
        task_id='run_nbb_pdf_scraper',
        python_callable=process_nbb_pdf,
    )