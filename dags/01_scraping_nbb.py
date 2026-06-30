import time
import requests
import logging
from itertools import cycle
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# L'import résout le chemin grâce au sous-dossier inclus dans dags/
from utils.db_utils import (
    get_active_bce_numbers, is_downloaded, mark_downloaded, 
    get_hdfs_client, write_to_hdfs
)

BASE = "https://consult.cbso.nbb.be/api"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*"
}

PROXIES = [
    {"http": "socks5h://tor1:9050", "https": "socks5h://tor1:9050"},
    {"http": "socks5h://tor2:9052", "https": "socks5h://tor2:9052"}
]
proxy_pool = cycle(PROXIES)

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.proxies.update(next(proxy_pool))
    return session

def get_deposits(session: requests.Session, enterprise_number: str) -> list:
    url = f"{BASE}/rs-consult/published-deposits?page=0&size=50&enterpriseNumber={enterprise_number}&sort=periodEndDate,desc"
    session.headers.update({"Referer": f"https://consult.cbso.nbb.be/consult-enterprise/{enterprise_number}"})
    session.get(f"https://consult.cbso.nbb.be/consult-enterprise/{enterprise_number}")
    
    r = session.get(url, timeout=15)
    if r.status_code == 429:
        logging.warning("HTTP 429 détecté. Rotation proxy.")
        session.proxies.update(next(proxy_pool))
        r = session.get(url, timeout=15)
        
    r.raise_for_status()
    return r.json().get("content", [])

def process_nbb():
    bce_list = get_active_bce_numbers()
    hdfs = get_hdfs_client()
    session = make_session()

    for bce in bce_list:
        logging.info(f"NBB | Traitement BCE: {bce}")
        try:
            deposits = get_deposits(session, bce)
        except Exception as e:
            logging.error(f"Echec dépôts {bce}: {e}")
            continue

        for dep in deposits:
            deposit_id = dep["id"]
            year = dep["periodEndDateYear"]

            if is_downloaded(bce, deposit_id, "COMPTE_ANNUEL"):
                continue

            hdfs_dir = f"/donnees_entreprises/{bce}/Compte_annuel/{year}"
            
            try:
                r_pdf = session.get(f"{BASE}/external/broker/public/deposits/pdf/{deposit_id}", timeout=30)
                r_pdf.raise_for_status()
                write_to_hdfs(hdfs, f"{hdfs_dir}/{bce}_{year}_{deposit_id}.pdf", r_pdf.content)
            except Exception as e:
                logging.error(f"Echec I/O PDF {deposit_id}: {e}")
                continue

            if not dep.get("migration"):
                try:
                    r_csv = session.get(f"{BASE}/external/broker/public/deposits/consult/csv/{deposit_id}", timeout=30)
                    r_csv.raise_for_status()
                    write_to_hdfs(hdfs, f"{hdfs_dir}/{bce}_{year}_{deposit_id}.csv", r_csv.content)
                except Exception as e:
                    logging.error(f"Echec I/O CSV {deposit_id}: {e}")

            mark_downloaded(bce, deposit_id, "COMPTE_ANNUEL", year, hdfs_dir)
            time.sleep(0.3)

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
    description='Extraction NBB',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nbb', 'scraping'],
) as dag:

    scraping_nbb_task = PythonOperator(
        task_id='run_nbb_scraper',
        python_callable=process_nbb,
    )