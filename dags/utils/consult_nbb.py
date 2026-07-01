import time
import random
import requests
import logging
from itertools import cycle
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
proxy_pool = cycle(PROXIES)

def get_new_session() -> requests.Session:
    """Force la destruction du pool de connexions via une nouvelle instance."""
    session = requests.Session()
    session.headers.update(HEADERS)
    proxy = next(proxy_pool)
    session.proxies.update(proxy)
    logging.info(f"[ROUTAGE] Bascule forcée sur le noeud : {proxy['http']}")
    return session

def fetch_with_retry(session: requests.Session, url: str, referer: str = None, retries: int = 5):
    """Wrapper transactionnel : intercepte les blocages API (429) et les défaillances SOCKS."""
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
            logging.warning(f"Défaillance réseau/SOCKS (Essai {attempt+1}/{retries}) : {str(e)[:80]}. Bascule noeud.")
            session = get_new_session()
            time.sleep(random.uniform(1.0, 2.0))
            
    raise Exception(f"Echec critique d'accès après {retries} tentatives : {url}")


def process_nbb_csv():
    hdfs = get_hdfs_client()
    session = get_new_session()

    while True:
        bce_list = get_pending_bce_numbers(limit=100)
        if not bce_list:
            logging.info("File d'attente d'orchestration épuisée.")
            break

        for bce_raw in bce_list:
            bce_clean = str(bce_raw).replace(".", "")
            
            api_url = f"{BASE}/rs-consult/published-deposits?page=0&size=50&enterpriseNumber={bce_clean}&sort=periodEndDate,desc"
            referer_url = f"https://consult.cbso.nbb.be/consult-enterprise/{bce_clean}"
            
            try:
                r, session = fetch_with_retry(session, api_url, referer=referer_url)
                deposits = r.json().get("content", [])
            except Exception as e:
                logging.error(f"Abandon extraction entité {bce_clean}: {e}")
                mark_bce_status(bce_raw, "pending")
                continue

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

                time.sleep(random.uniform(0.5, 1.5))

            mark_bce_status(bce_raw, "done")