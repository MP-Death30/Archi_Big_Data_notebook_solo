import pymongo

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "kbo_db"
COL_QUEUE = "scraping_queue"

def reset_in_progress():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    result = db[COL_QUEUE].update_many(
        {"status": "in_progress"},
        {"$set": {"status": "pending"}}
    )
    
    print(f"Entités réinitialisées (in_progress -> pending) : {result.modified_count}")

if __name__ == "__main__":
    reset_in_progress()