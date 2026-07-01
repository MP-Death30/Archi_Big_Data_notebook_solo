import pymongo
from hdfs import InsecureClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "kbo_db"
COL_QUEUE = "scraping_queue"
HDFS_URL = "http://localhost:9870" 
HDFS_BASE_PATH = "/donnees_entreprises"

def requeue_missing_entities():
    # 1. Capture de l'empreinte physique (HDFS)
    hdfs_client = InsecureClient(HDFS_URL, user='root')
    try:
        hdfs_folders = set(hdfs_client.list(HDFS_BASE_PATH))
    except Exception as e:
        print(f"Échec d'accès HDFS : {e}")
        return

    # 2. Extraction du référentiel et identification du delta
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    
    raw_bces = [doc["_id"] for doc in db[COL_QUEUE].find({}, {"_id": 1})]
    
    to_requeue = []
    for raw_bce in raw_bces:
        clean_bce = str(raw_bce).replace(".", "")
        if clean_bce not in hdfs_folders:
            to_requeue.append(raw_bce)

    if not to_requeue:
        print("Aucune entité manquante détectée.")
        
    else:
        print(f"Volume identifié pour réinjection : {len(to_requeue)}")
        
        # 3. Réinitialisation ciblée
        result = db[COL_QUEUE].update_many(
            {"_id": {"$in": to_requeue}},
            {"$set": {"status": "pending"}}
        )
        print(f"Mise à jour MongoDB (manquants -> pending) : {result.modified_count}")

    # 4. Purge des états transitionnels bloqués
    res_stuck = db[COL_QUEUE].update_many(
        {"status": "in_progress"},
        {"$set": {"status": "pending"}}
    )
    print(f"Mise à jour MongoDB (in_progress -> pending) : {res_stuck.modified_count}")

if __name__ == "__main__":
    requeue_missing_entities()