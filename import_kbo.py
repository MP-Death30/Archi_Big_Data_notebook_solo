import csv
import os
from pymongo import MongoClient

# Configuration
MONGO_URI = "mongodb://localhost:27017/" # Modifier par "mongodb://mongodb:27017/" si exécuté dans un conteneur
DB_NAME = "kbo_db"
DATA_DIR = "./data/KBO/"
BATCH_SIZE = 10000

FILES = [
    "enterprise.csv",
    "activity.csv",
    "addess.csv", 
    "branch.csv",
    "code.csv",
    "contact.csv"
]

def import_csv_to_mongo():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    for filename in FILES:
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            print(f"Ignoré (introuvable) : {filepath}")
            continue

        collection_name = filename.split('.')[0]
        collection = db[collection_name]
        
        collection.drop()
        print(f"Importation en cours : {filename} vers la collection '{collection_name}'...")

        batch = []
        count = 0

        with open(filepath, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    collection.insert_many(batch)
                    count += len(batch)
                    batch.clear()
                    print(f"  -> {count} lignes insérées")

            if batch:
                collection.insert_many(batch)
                count += len(batch)
                
        print(f"Terminé pour {filename}. Total: {count} documents.\n")

if __name__ == "__main__":
    import_csv_to_mongo()