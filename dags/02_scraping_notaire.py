import sys
import time
import random
import logging
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException
from playwright.sync_api import sync_playwright

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Injection du chemin pour résoudre le dossier utils interne aux dags
sys.path.insert(0, '/opt/airflow/dags')
from utils.db_utils import (
    get_pending_bce_numbers, mark_bce_status, 
    is_downloaded, mark_downloaded, 
    get_hdfs_client, write_to_hdfs
)

BASE = "https://statuts.notaire.be/stapor_v1"
SEED_BCE = "0836157420"
HEADERS_API = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*"
}

PROXIES = [
    {"http": "socks5h://tor1:9050", "https": "socks5h://tor1:9050"},
    {"http": "socks5h://tor2:9050", "https": "socks5h://tor2:9050"},
    {"http": "socks5h://tor3:9050", "https": "socks5h://tor3:9050"},
    {"http": "socks5h://tor4:9050", "https": "socks5h://tor4:9050"},
    {"http": "socks5h://tor5:9050", "https": "socks5h://tor5:9050"},
    {"http": "socks5h://tor6:9050", "https": "socks5h://tor6:9050"}
]

def _fetch_cookies_via_playwright(proxy_dict: dict) -> list[dict]:
    """Résout le challenge F5 en liant strictement l'instance Chromium au nœud Tor."""
    with sync_playwright() as p:
        pw_proxy = {"server": proxy_dict["http"].replace("socks5h", "socks5")}
        browser = p.chromium.launch(
            headless=True, 
            proxy=pw_proxy, 
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        ctx = browser.new_context(user_agent=HEADERS_API["User-Agent"])
        page = ctx.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page.goto("https://statuts.notaire.be/", wait_until="load", timeout=60000)
        page.wait_for_timeout(2000)
        
        seed_url = f"{BASE}/enterprise/{SEED_BCE}/statutes?enterpriseNumber={SEED_BCE}&statuteStart=0&statuteCount=5"
        page.goto(seed_url, wait_until="load", timeout=60000)
        
        for _ in range(40):
            names = {c["name"] for c in ctx.cookies()}
            if "OClmoOot" in names and "Lyp1CWKh" in names:
                break
            page.wait_for_timeout(500)
            
        cookies = ctx.cookies()
        browser.close()
    return cookies

def get_new_session() -> requests.Session:
    """Génère une session HTTP couplée à une IP Tor fraîche et ses cookies F5 autorisés."""
    proxy = random.choice(PROXIES)
    logging.info(f"[ROUTAGE NOTAIRE] Résolution F5 sur nœud : {proxy['http']}")
    cookies = _fetch_cookies_via_playwright(proxy)

    session = requests.Session()
    session.headers.update(HEADERS_API)
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c["domain"])
    session.proxies.update(proxy)
    return session

def fetch_with_retry(session: requests.Session, url: str, params: dict = None, referer: str = None, retries: int = 3):
    """Intercepte les rejets d'empreinte (403/429) et force le renouvellement cryptographique (Playwright)."""
    if referer:
        session.headers.update({"Referer": referer})

    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code in [403, 429, 503]:
                logging.warning(f"Blocage F5 {r.status_code} (Essai {attempt+1}/{retries}). Renouvellement Playwright.")
                session = get_new_session()
                time.sleep(random.uniform(2.0, 5.0))
                continue
            r.raise_for_status()
            return r, session
        except RequestException as e:
            logging.warning(f"Défaillance réseau (Essai {attempt+1}/{retries}) : {str(e)[:60]}. Bascule.")
            session = get_new_session()
            time.sleep(random.uniform(2.0, 5.0))

    raise Exception(f"Echec critique d'accès après {retries} tentatives : {url}")

def process_single_notaire(bce_raw: str):
    hdfs = get_hdfs_client()
    bce_clean = str(bce_raw).replace(".", "")
    
    try:
        session = get_new_session()
    except Exception as e:
        logging.error(f"Echec initialisation Playwright pour {bce_raw}: {e}")
        mark_bce_status(bce_raw, "pending")
        return False

    url = f"{BASE}/api/enterprises/{bce_clean}/statutes"
    referer = f"{BASE}/enterprise/{bce_clean}/statutes?enterpriseNumber={bce_clean}&statuteStart=0&statuteCount=5"
    
    offset = 0
    while True:
        try:
            r, session = fetch_with_retry(session, url, params={"deedDate": "", "offset": offset, "limit": 20}, referer=referer)
            data = r.json()
            statutes = data.get("statutes", [])
        except Exception as e:
            logging.error(f"Abandon extraction entité {bce_clean}: {e}")
            mark_bce_status(bce_raw, "pending")
            return False

        for s in statutes:
            if s.get("documentStatus") != "DONE":
                continue
                
            doc_id = s["documentId"]
            deed_date = s.get("deedDate", "1970-01-01")
            year = int(deed_date.split("-")[0])
            
            if is_downloaded(bce_clean, doc_id, "NOTAIRE"):
                continue

            hdfs_dir = f"/donnees_entreprises/{bce_clean}/Notaire/{year}"
            pdf_url = f"{BASE}/api/enterprises/{bce_clean}/statutes/non-certified/{doc_id}"
            
            try:
                r_pdf, session = fetch_with_retry(session, pdf_url)
                pdf_path = f"{hdfs_dir}/{bce_clean}_{deed_date.replace('-', '')}_{doc_id}.pdf"
                write_to_hdfs(hdfs, pdf_path, r_pdf.content)
                mark_downloaded(bce_clean, doc_id, "NOTAIRE", year, hdfs_dir)
                logging.info(f"I/O HDFS (Notaire) : {bce_clean} | {year}")
            except Exception as e:
                logging.error(f"Echec persistance PDF {doc_id}: {e}")

        if len(statutes) < 20:
            break
        offset += 20
        time.sleep(random.uniform(2.0, 5.0))

    mark_bce_status(bce_raw, "done")
    return True

def process_notaire_dag():
    # Limite stricte : Chromium consomme ~200MB/instance. Max 3 threads garantit la stabilité du worker Airflow.
    MAX_WORKERS = 3  
    
    while True:
        bce_list = get_pending_bce_numbers(limit=15)
        if not bce_list:
            logging.info("File d'attente d'orchestration Notaire épuisée.")
            break

        logging.info(f"Injection batch ({len(bce_list)} entités) dans pool ({MAX_WORKERS} threads).")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_notaire, bce): bce for bce in bce_list}
            
            for future in as_completed(futures):
                bce_raw = futures[future]
                try:
                    success = future.result()
                    if not success:
                        logging.warning(f"Exécution incomplète pour : {bce_raw}")
                except Exception as e:
                    logging.error(f"Crash thread sur {bce_raw} : {e}")
                    mark_bce_status(bce_raw, "pending")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    '02_scraping_notaire',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    PythonOperator(
        task_id='run_notaire_scraper',
        python_callable=process_notaire_dag,
    )