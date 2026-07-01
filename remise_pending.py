import pymongo

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "kbo_db"
COL_QUEUE = "scraping_queue"

def reset_all_to_pending():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    result = db[COL_QUEUE].update_many(
        {}, 
        {"$set": {"status": "pending"}}
    )
    
    print(f"Entités réinitialisées (-> pending) : {result.modified_count}")

if __name__ == "__main__":
    reset_all_to_pending()