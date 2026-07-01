import pymongo
from hdfs import InsecureClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "kbo_db"
COL_QUEUE = "scraping_queue"
# Adapter HDFS_URL si exécuté depuis l'hôte (localhost:9870) ou un conteneur (namenode:9870)
HDFS_URL = "http://localhost:9870" 
HDFS_BASE_PATH = "/donnees_entreprises"

def audit_hdfs_consistency():
    # 1. Extraction du référentiel cible (MongoDB)
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    
    # Récupération de l'intégralité de la file d'attente (tous statuts confondus)
    raw_bces = [doc["_id"] for doc in db[COL_QUEUE].find({}, {"_id": 1})]
    expected_bces = set(str(bce).replace(".", "") for bce in raw_bces)
    
    if not expected_bces:
        print("Erreur : La collection scraping_queue est vide.")
        return

    # 2. Extraction du stockage physique (HDFS)
    hdfs_client = InsecureClient(HDFS_URL, user='root')
    
    try:
        hdfs_folders = hdfs_client.list(HDFS_BASE_PATH)
        actual_bces = set(hdfs_folders)
    except Exception as e:
        print(f"Échec de connexion ou lecture HDFS : {e}")
        return

    # 3. Calcul de la variance
    missing_in_hdfs = expected_bces - actual_bces
    orphans_in_hdfs = actual_bces - expected_bces

    # 4. Rapport d'état
    print(f"Cibles MongoDB       : {len(expected_bces)}")
    print(f"Dossiers HDFS        : {len(actual_bces)}")
    print(f"Déficit HDFS         : {len(missing_in_hdfs)}")
    
    if missing_in_hdfs:
        print("\nÉchantillon des entités manquantes (Top 20) :")
        print(list(missing_in_hdfs)[:20])
        
    if orphans_in_hdfs:
        print(f"\nDossiers orphelins dans HDFS (non traqués dans MongoDB) : {len(orphans_in_hdfs)}")

if __name__ == "__main__":
    audit_hdfs_consistency()