import pymongo

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "kbo_db"
COL_QUEUE = "scraping_queue"

def check_status_counts():
    client = pymongo.MongoClient(MONGO_URI)
    pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    results = list(client[DB_NAME][COL_QUEUE].aggregate(pipeline))
    
    print("Répartition dans scraping_queue :")
    for r in results:
        print(f"[{r.get('_id', 'NULL')}] : {r['count']}")

if __name__ == "__main__":
    check_status_counts()