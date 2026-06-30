import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Résolution du chemin d'accès aux modules Python personnalisés
sys.path.append('/opt/airflow')
from scripts.consult_nbb import process_nbb

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    '01_scraping_nbb',
    default_args=default_args,
    description='Extraction des PDF et CSV depuis l\'API NBB avec proxy Tor',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nbb', 'scraping', 'pdf', 'csv', 'tor'],
) as dag:

    scraping_nbb_task = PythonOperator(
        task_id='run_nbb_scraper',
        python_callable=process_nbb,
    )