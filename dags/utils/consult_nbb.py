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
        session.proxies.update(next(proxy_pool))
        r = session.get(url, timeout=15)
        
    r.raise_for_status()
    return r.json().get("content", [])

def process_nbb_csv():
    logging.info("PROCESS | Démarrage du DAG 01a_scraping_nbb_csv")
    
    try:
        logging.info("PROCESS | Étape 1 : Initialisation des dépendances")
        bce_list = get_active_bce_numbers()
        if not bce_list:
            logging.warning("PROCESS | Liste BCE vide. Arrêt prématuré.")
            return
            
        hdfs = get_hdfs_client()
        session = make_session()
        logging.info("PROCESS | Étape 1 terminée avec succès.")
    except Exception as e:
        logging.error(f"PROCESS | Échec critique d'initialisation : {str(e)}", exc_info=True)
        raise

    for idx, bce in enumerate(bce_list):
        logging.info(f"NBB CSV | [{idx+1}/{len(bce_list)}] Traitement BCE: {bce}")
        try:
            deposits = get_deposits(session, bce)
            logging.info(f"NBB CSV | {len(deposits)} dépôts trouvés pour {bce}")
        except requests.exceptions.RequestException as req_e:
            logging.error(f"NBB CSV | Timeout ou erreur réseau API pour {bce}: {str(req_e)}")
            continue
        except Exception as e:
            logging.error(f"NBB CSV | Échec inattendu requêtage dépôts {bce}: {str(e)}", exc_info=True)
            continue

        for dep in deposits:
            if dep.get("migration"):
                continue

            deposit_id = dep["id"]
            year = dep.get("periodEndDateYear", "UNKNOWN")

            if is_downloaded(bce, deposit_id, "COMPTE_ANNUEL_CSV"):
                logging.info(f"NBB CSV | Dépôt {deposit_id} (Année: {year}) déjà téléchargé. Ignoré.")
                continue

            hdfs_dir = f"/donnees_entreprises/{bce}/Compte_annuel/{year}"
            csv_url = f"{BASE}/external/broker/public/deposits/consult/csv/{deposit_id}"
            
            logging.info(f"NBB CSV | Requête GET CSV : {csv_url}")
            try:
                r_csv = session.get(csv_url, timeout=30)
                r_csv.raise_for_status()
                
                write_to_hdfs(hdfs, f"{hdfs_dir}/{bce}_{year}_{deposit_id}.csv", r_csv.content)
                mark_downloaded(bce, deposit_id, "COMPTE_ANNUEL_CSV", year, hdfs_dir)
                logging.info(f"NBB CSV | Succès intégration CSV: {deposit_id}")
                time.sleep(0.3)
            except requests.exceptions.HTTPError as http_e:
                logging.error(f"NBB CSV | Échec HTTP {r_csv.status_code} pour CSV {deposit_id}")
            except Exception as e:
                logging.error(f"NBB CSV | Échec I/O global pour CSV {deposit_id}: {str(e)}", exc_info=True)
                
    logging.info("PROCESS | Fin d'exécution du DAG 01a_scraping_nbb_csv")