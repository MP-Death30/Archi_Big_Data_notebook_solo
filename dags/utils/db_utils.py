import os
import logging
from datetime import datetime
from pymongo import MongoClient
from hdfs import InsecureClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "airflow")

def get_mongo_client() -> MongoClient:
    logging.info(f"DB_UTILS | Tentative de connexion MongoDB via {MONGO_URI}")
    # serverSelectionTimeoutMS évite un blocage infini si MongoDB est down
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def get_hdfs_client() -> InsecureClient:
    logging.info(f"DB_UTILS | Tentative de connexion HDFS via {HDFS_URL}")
    return InsecureClient(HDFS_URL, user=HDFS_USER)

def get_active_bce_numbers(limit: int = None) -> list[str]:
    logging.info("DB_UTILS | Récupération des numéros BCE actifs...")
    client = get_mongo_client()
    db = client["kbo_db"]
    
    # Remplacement strict de la clause de filtrage
    query = {"Status": "AC"} 
    
    collection_name = "enterprises_rich" if "enterprises_rich" in db.list_collection_names() else "enterprises"
    logging.info(f"DB_UTILS | Utilisation de la collection : {collection_name}")
    
    cursor = db[collection_name].find(query, {"_id": 1})
    if limit:
        cursor = cursor.limit(limit)
        
    result = [doc["_id"] for doc in cursor]
    logging.info(f"DB_UTILS | {len(result)} numéros BCE chargés en mémoire.")
    return result

def is_downloaded(bce_number: str, deposit_id: str, source: str) -> bool:
    client = get_mongo_client()
    db = client["state_db"]
    doc = db["downloads_tracking"].find_one({
        "bce_number": bce_number,
        "deposit_id": deposit_id,
        "source": source,
        "status": "DONE"
    })
    return doc is not None

def mark_downloaded(bce_number: str, deposit_id: str, source: str, year: int, hdfs_path: str) -> None:
    client = get_mongo_client()
    db = client["state_db"]
    db["downloads_tracking"].update_one(
        {"bce_number": bce_number, "deposit_id": deposit_id, "source": source},
        {"$set": {
            "bce_number": bce_number,
            "deposit_id": deposit_id,
            "source": source,
            "year": year,
            "hdfs_path": hdfs_path,
            "status": "DONE",
            "timestamp": datetime.utcnow()
        }},
        upsert=True
    )

def write_to_hdfs(client: InsecureClient, hdfs_path: str, data: bytes) -> None:
    logging.info(f"DB_UTILS | Écriture HDFS cible: {hdfs_path} ({len(data)} bytes)")
    client.write(hdfs_path, data, overwrite=True)