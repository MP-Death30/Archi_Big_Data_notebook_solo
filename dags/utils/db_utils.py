import pymongo
from datetime import datetime
from hdfs import InsecureClient

MONGO_URI = "mongodb://mongodb:27017/"

def get_mongo_client():
    return pymongo.MongoClient(MONGO_URI)

# ── ORCHESTRATION (Niveau Entreprise) ───────────────────────────────────────
def get_pending_bce_numbers(limit=50):
    client = get_mongo_client()
    db = client["kbo_db"]
    
    docs = list(db["scraping_queue"].find({"status": "pending"}).limit(limit))
    bce_list = [d["_id"] for d in docs]
    
    if bce_list:
        db["scraping_queue"].update_many(
            {"_id": {"$in": bce_list}},
            {"$set": {"status": "in_progress", "updated_at": datetime.utcnow()}}
        )
    return bce_list

def mark_bce_status(bce: str, status: str):
    client = get_mongo_client()
    db = client["kbo_db"]
    db["scraping_queue"].update_one(
        {"_id": bce},
        {"$set": {"status": status, "updated_at": datetime.utcnow()}}
    )

# ── IDEMPOTENCE (Niveau Document) ───────────────────────────────────────────
def is_downloaded(bce_number: str, deposit_id: str, source: str) -> bool:
    client = get_mongo_client()
    db = client["kbo_db"]
    doc = db["state_db"].find_one({
        "bce_number": bce_number, 
        "deposit_id": deposit_id, 
        "source": source
    })
    return doc is not None

def mark_downloaded(bce_number: str, deposit_id: str, source: str, year: int, hdfs_path: str) -> None:
    client = get_mongo_client()
    db = client["kbo_db"]
    db["state_db"].update_one(
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

# ── HDFS ────────────────────────────────────────────────────────────────────
def get_hdfs_client():
    return InsecureClient('http://namenode:9870', user='root')

def write_to_hdfs(client, hdfs_path, content):
    with client.write(hdfs_path, overwrite=True) as writer:
        writer.write(content)