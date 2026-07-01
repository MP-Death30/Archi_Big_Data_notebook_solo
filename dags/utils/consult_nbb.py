import time
import random
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException

from utils.db_utils import (
    get_pending_bce_numbers, mark_bce_status, 
    is_downloaded, mark_downloaded, 
    get_hdfs_client, write_to_hdfs
)

BASE = "https://consult.cbso.nbb.be/api"
HEADERS = {
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

def get_new_session() -> requests.Session:
    """Instanciation thread-safe avec assignation proxy aléatoire."""
    session = requests.Session()
    session.headers.update(HEADERS)
    proxy = random.choice(PROXIES)
    session.proxies.update(proxy)
    logging.debug(f"[ROUTAGE] Assignation nœud : {proxy['http']}")
    return session

def fetch_with_retry(session: requests.Session, url: str, referer: str = None, retries: int = 5):
    if referer:
        session.headers.update({"Referer": referer})
        try:
            session.get(referer, timeout=10)
        except RequestException:
            pass 
            
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 429:
                logging.warning(f"Blocage 429 (Essai {attempt+1}/{retries}). Renouvellement du circuit.")
                session = get_new_session()
                time.sleep(random.uniform(2.0, 5.0))
                continue
            r.raise_for_status()
            return r, session
        except RequestException as e:
            logging.warning(f"Défaillance SOCKS/Réseau (Essai {attempt+1}/{retries}) : {str(e)[:60]}. Bascule.")
            session = get_new_session()
            time.sleep(random.uniform(2.0, 5.0))
            
    raise Exception(f"Echec critique d'accès après {retries} tentatives : {url}")

def process_single_bce(bce_raw: str):
    """Fonction atomique exécutée par chaque thread pour une entité."""
    # Isolation des clients I/O
    hdfs = get_hdfs_client()
    session = get_new_session()
    
    bce_clean = str(bce_raw).replace(".", "")
    api_url = f"{BASE}/rs-consult/published-deposits?page=0&size=50&enterpriseNumber={bce_clean}&sort=periodEndDate,desc"
    referer_url = f"https://consult.cbso.nbb.be/consult-enterprise/{bce_clean}"
    
    try:
        r, session = fetch_with_retry(session, api_url, referer=referer_url)
        deposits = r.json().get("content", [])
    except Exception as e:
        logging.error(f"Abandon extraction entité {bce_clean}: {e}")
        mark_bce_status(bce_raw, "pending")
        return False

    for dep in deposits:
        deposit_id = dep["id"]
        year = dep.get("periodEndDateYear")
        
        if not year or int(year) < 2021:
            continue
            
        if dep.get("migration"):
            continue

        if is_downloaded(bce_clean, deposit_id, "COMPTE_ANNUEL"):
            continue

        hdfs_dir = f"/donnees_entreprises/{bce_clean}/Compte_annuel/{year}"
        csv_url = f"{BASE}/external/broker/public/deposits/consult/csv/{deposit_id}"
        
        try:
            r_csv, session = fetch_with_retry(session, csv_url)
            write_to_hdfs(hdfs, f"{hdfs_dir}/{bce_clean}_{year}_{deposit_id}.csv", r_csv.content)
            mark_downloaded(bce_clean, deposit_id, "COMPTE_ANNUEL", year, hdfs_dir)
            logging.info(f"I/O HDFS : {bce_clean} | {year}")
        except Exception as e:
            logging.error(f"Echec persistance I/O {deposit_id}: {e}")

        time.sleep(random.uniform(2.0, 5.0))

    mark_bce_status(bce_raw, "done")
    return True

def process_nbb_csv():
    # Correspondance architecturale : 1 thread maximum par conteneur Tor disponible
    MAX_WORKERS = 6 
    
    while True:
        bce_list = get_pending_bce_numbers(limit=50)
        if not bce_list:
            logging.info("File d'attente d'orchestration épuisée.")
            break

        logging.info(f"Injection batch ({len(bce_list)} entités) dans pool d'exécution ({MAX_WORKERS} threads).")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_bce, bce): bce for bce in bce_list}
            
            for future in as_completed(futures):
                bce_raw = futures[future]
                try:
                    success = future.result()
                    if not success:
                        logging.warning(f"Exécution incomplète pour : {bce_raw}")
                except Exception as e:
                    logging.error(f"Crash d'exécution de thread sur {bce_raw} : {e}")
                    mark_bce_status(bce_raw, "pending")