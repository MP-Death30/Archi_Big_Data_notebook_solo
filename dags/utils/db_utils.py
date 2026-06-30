import os
from datetime import datetime
from pymongo import MongoClient
from hdfs import InsecureClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "airflow")

def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)

def get_hdfs_client() -> InsecureClient:
    return InsecureClient(HDFS_URL, user=HDFS_USER)

def get_active_bce_numbers(limit: int = None) -> list[str]:
    client = get_mongo_client()
    db = client["kbo_db"]
    query = {"status": "Active"}
    cursor = db["enterprises"].find(query, {"_id": 1})
    if limit:
        cursor = cursor.limit(limit)
    return [doc["_id"] for doc in cursor]

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
    client.write(hdfs_path, data, overwrite=True)