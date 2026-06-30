import json
import time
import logging
import requests
from itertools import cycle
from pathlib import Path
from playwright.sync_api import sync_playwright
from scripts.db_utils import (
    get_active_bce_numbers, is_downloaded, mark_downloaded, 
    get_hdfs_client, write_to_hdfs
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE = "https://statuts.notaire.be/stapor_v1"
COOKIE_FILE = Path("/opt/airflow/data/notaire_cookies.json")
SEED_BCE = "0836157420"
HEADERS_API = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*"
}

PROXIES = [
    {"http": "socks5h://tor1:9050", "https": "socks5h://tor1:9050"},
    {"http": "socks5h://tor2:9052", "https": "socks5h://tor2:9052"}
]
proxy_pool = cycle(PROXIES)

def _fetch_cookies_via_playwright() -> list[dict]:
    logging.info("Génération tokens F5 (Playwright Headless)")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        ctx = browser.new_context(user_agent=HEADERS_API["User-Agent"])
        page = ctx.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page.goto("https://statuts.notaire.be/", wait_until="load", timeout=30000)
        page.wait_for_timeout(2000)
        
        seed_url = f"{BASE}/enterprise/{SEED_BCE}/statutes?enterpriseNumber={SEED_BCE}&statuteStart=0&statuteCount=5"
        page.goto(seed_url, wait_until="load", timeout=30000)
        
        for _ in range(20):
            names = {c["name"] for c in ctx.cookies()}
            if "OClmoOot" in names and "Lyp1CWKh" in names:
                break
            page.wait_for_timeout(500)
            
        cookies = ctx.cookies()
        browser.close()
    return cookies

def get_session() -> requests.Session:
    if COOKIE_FILE.exists():
        cookies = json.loads(COOKIE_FILE.read_text())
        session = requests.Session()
        session.headers.update(HEADERS_API)
        for c in cookies:
            session.cookies.set(c["name"], c["value"], domain=c["domain"])
        
        try:
            r = session.get(f"{BASE}/api/enterprises/{SEED_BCE}/statutes", params={"offset": 0, "limit": 1}, timeout=10)
            if "application/json" in r.headers.get("content-type", ""):
                session.proxies.update(next(proxy_pool))
                return session
        except Exception:
            pass

    cookies = _fetch_cookies_via_playwright()
    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
    COOKIE_FILE.write_text(json.dumps(cookies))
    
    session = requests.Session()
    session.headers.update(HEADERS_API)
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c["domain"])
    session.proxies.update(next(proxy_pool))
    return session

def process_notaire():
    bce_list = get_active_bce_numbers()
    hdfs = get_hdfs_client()
    session = get_session()

    for bce in bce_list:
        logging.info(f"Notaire | Traitement BCE: {bce}")
        url = f"{BASE}/api/enterprises/{bce}/statutes"
        session.headers["Referer"] = f"{BASE}/enterprise/{bce}/statutes?enterpriseNumber={bce}&statuteStart=0&statuteCount=5"
        
        offset = 0
        while True:
            r = session.get(url, params={"deedDate": "", "offset": offset, "limit": 20}, timeout=15)
            if r.status_code == 429:
                session.proxies.update(next(proxy_pool))
                continue
            r.raise_for_status()
            
            statutes = r.json().get("statutes", [])
            for s in statutes:
                if s.get("documentStatus") != "DONE":
                    continue
                    
                doc_id = s["documentId"]
                deed_date = s.get("deedDate", "1970-01-01")
                year = int(deed_date.split("-")[0])
                
                if is_downloaded(bce, doc_id, "NOTAIRE"):
                    continue

                hdfs_dir = f"/donnees_entreprises/{bce}/Notaire/{year}"
                pdf_url = f"{BASE}/api/enterprises/{bce}/statutes/non-certified/{doc_id}"
                
                try:
                    r_pdf = session.get(pdf_url, timeout=30)
                    if r_pdf.status_code == 200 and len(r_pdf.content) > 1000:
                        pdf_path = f"{hdfs_dir}/{bce}_{deed_date.replace('-', '')}_{doc_id}.pdf"
                        write_to_hdfs(hdfs, pdf_path, r_pdf.content)
                        mark_downloaded(bce, doc_id, "NOTAIRE", year, hdfs_dir)
                except Exception as e:
                    logging.error(f"Echec I/O PDF {doc_id}: {e}")
            
            if len(statutes) < 20:
                break
            offset += 20
            time.sleep(0.3)

if __name__ == "__main__":
    process_notaire()