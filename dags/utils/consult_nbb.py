import time
import requests
import logging
from itertools import cycle
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
    {"http": "socks5h://tor1:9150", "https": "socks5h://tor1:9150"},
    {"http": "socks5h://tor2:9150", "https": "socks5h://tor2:9150"}
]
proxy_pool = cycle(PROXIES)

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.proxies.update(next(proxy_pool))
    return session

def safe_request(session: requests.Session, url: str, max_retries: int = 4) -> requests.Response:
    for attempt in range(max_retries):
        try:
            # Freinage absolu : 30 requêtes par minute
            time.sleep(2) 
            
            r = session.get(url, timeout=30)
            if r.status_code == 429:
                logging.warning(f"HTTP 429 détecté. Rotation IP Tor imminente.")
                session.proxies.update(next(proxy_pool))
                time.sleep(10) # Temps de latence pour établissement du nouveau circuit
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.ConnectionError:
            logging.warning("Errno 111: Déni de service local du proxy. Rotation forcée.")
            session.proxies.update(next(proxy_pool))
            time.sleep(10)
        except Exception as e:
            logging.error(f"Échec réseau sur l'essai {attempt+1}: {str(e)}")
            time.sleep(5)
            
    raise Exception(f"Blocage définitif sur l'URL: {url}")

def get_deposits(session: requests.Session, enterprise_number: str) -> list:
    url = f"{BASE}/rs-consult/published-deposits?page=0&size=50&enterpriseNumber={enterprise_number}&sort=periodEndDate,desc"
    session.headers.update({"Referer": f"https://consult.cbso.nbb.be/consult-enterprise/{enterprise_number}"})
    safe_request(session, f"https://consult.cbso.nbb.be/consult-enterprise/{enterprise_number}")
    r = safe_request(session, url)
    return r.json().get("content", [])

def process_nbb_csv():
    bce_list = get_active_bce_numbers()
    hdfs = get_hdfs_client()
    session = make_session()

    for idx, bce in enumerate(bce_list):
        if idx % 100 == 0:
            logging.info(f"Progression : {idx} / {len(bce_list)}")
            
        try:
            deposits = get_deposits(session, bce)
        except Exception as e:
            logging.error(f"Abandon des dépôts pour {bce}: {e}")
            continue

        for dep in deposits:
            if dep.get("migration"):
                continue

            deposit_id = dep["id"]
            year = dep.get("periodEndDateYear", "UNKNOWN")

            if is_downloaded(bce, deposit_id, "COMPTE_ANNUEL_CSV"):
                continue

            hdfs_dir = f"/donnees_entreprises/{bce}/Compte_annuel/{year}"
            csv_url = f"{BASE}/external/broker/public/deposits/consult/csv/{deposit_id}"
            
            try:
                r_csv = safe_request(session, csv_url)
                write_to_hdfs(hdfs, f"{hdfs_dir}/{bce}_{year}_{deposit_id}.csv", r_csv.content)
                mark_downloaded(bce, deposit_id, "COMPTE_ANNUEL_CSV", year, hdfs_dir)
                logging.info(f"Intégration CSV validée: {deposit_id}")
            except Exception as e:
                logging.error(f"Rejet I/O final CSV {deposit_id}: {e}")