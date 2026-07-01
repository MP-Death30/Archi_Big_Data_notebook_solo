import pymongo
from datetime import datetime

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "kbo_db"
COL_SILVER = "enterprise_silver"
COL_QUEUE = "scraping_queue" # Redirection vers la queue d'orchestration

HOTEL_CODES = ["55100", "55201", "55202", "55203", "55204", "55209", "55300", "55400", "55900"]
EXCLUDED_JURIDICAL = [
    "110", "114", "116", "117", 
    "301", "302", "303", 
    "310", "320", "330", "340", "350", 
    "400", "411", "412", "413", "414", "415", "416", "417", "418", "419", "420"
]

def init_scraping_queue():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    pipeline = [
        {
            "$match": {
                "Status": "AC",
                "TypeOfEnterprise": "2",
                "JuridicalForm": {"$nin": EXCLUDED_JURIDICAL},
                "activites": {
                    "$elemMatch": {
                        "Classification": "MAIN",
                        "NaceCode": {"$in": HOTEL_CODES}
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "EnterpriseNumber": 1
            }
        }
    ]

    cursor = db[COL_SILVER].aggregate(pipeline)
    targets = [doc["EnterpriseNumber"] for doc in cursor if "EnterpriseNumber" in doc]

    if not targets:
        return

    bulk_ops = [
        pymongo.UpdateOne(
            {"_id": ent},
            {"$setOnInsert": {"status": "pending", "updated_at": datetime.utcnow()}},
            upsert=True
        )
        for ent in targets
    ]

    db[COL_QUEUE].bulk_write(bulk_ops)

if __name__ == "__main__":
    init_scraping_queue()