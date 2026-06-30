import csv
import sqlite3
import os
from pymongo import MongoClient, UpdateOne
from collections import defaultdict

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "kbo_db"
COLLECTION_NAME = "enterprises_rich"
DATA_DIR = "./data/KBO/"
BATCH_SIZE = 1000
SQLITE_DB = "staging_kbo.db"

FILES_MAPPING = {
    "enterprise": {"file": "enterprise.csv", "id_col": "EnterpriseNumber"},
    "activity": {"file": "activity.csv", "id_col": "EntityNumber"},
    "address": {"file": "address.csv", "id_col": "EntityNumber"}, 
    "branch": {"file": "branch.csv", "id_col": "EnterpriseNumber"},
    "contact": {"file": "contact.csv", "id_col": "EntityNumber"},
    "establishment": {"file": "establishment.csv", "id_col": "EnterpriseNumber"},
    "denomination": {"file": "denomination.csv", "id_col": "EntityNumber"}
}

def load_codes_mapping():
    filepath = os.path.join(DATA_DIR, "code.csv")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Arrêt critique. Fichier dictionnaire introuvable : {filepath}")
    
    codes = defaultdict(lambda: defaultdict(dict))
    with open(filepath, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cat = row["Category"]
            code = row["Code"]
            lang = row["Language"]
            codes[cat][code][lang] = row["Description"]
    print("Dictionnaire de codes chargé en mémoire.")
    return codes

def setup_sqlite():
    if os.path.exists(SQLITE_DB):
        os.remove(SQLITE_DB)
    
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    
    for table, meta in FILES_MAPPING.items():
        filepath = os.path.join(DATA_DIR, meta["file"])
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Arrêt critique. Fichier introuvable : {filepath}")
            
        with open(filepath, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            cols = ", ".join([f'"{h}" TEXT' for h in headers])
            cursor.execute(f'CREATE TABLE {table} ({cols})')
            
            placeholders = ", ".join(["?"] * len(headers))
            insert_query = f'INSERT INTO {table} VALUES ({placeholders})'
            
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= 100000:
                    cursor.executemany(insert_query, batch)
                    batch.clear()
            if batch:
                cursor.executemany(insert_query, batch)
                
        cursor.execute(f'CREATE INDEX idx_{table}_id ON {table}("{meta["id_col"]}")')
        print(f"Table SQLite '{table}' chargée et indexée.")
        
    conn.commit()
    return conn

def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def process_and_load_mongo(sqlite_conn, codes_dict):
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    sqlite_conn.row_factory = dict_factory
    cursor = sqlite_conn.cursor()
    
    cursor.execute('SELECT "EnterpriseNumber" FROM enterprise')
    all_enterprises = [row["EnterpriseNumber"] for row in cursor.fetchall()]
    total = len(all_enterprises)
    
    print(f"\nDébut de la mise à jour MongoDB ({total} entreprises)...")
    
    for i in range(0, total, BATCH_SIZE):
        batch_ids = all_enterprises[i:i+BATCH_SIZE]
        placeholders = ",".join(["?"] * len(batch_ids))
        
        cursor.execute(f'SELECT * FROM enterprise WHERE "EnterpriseNumber" IN ({placeholders})', batch_ids)
        enterprises_data = {row["EnterpriseNumber"]: row for row in cursor.fetchall()}
        
        cursor.execute(f'SELECT * FROM activity WHERE "EntityNumber" IN ({placeholders})', batch_ids)
        activities = defaultdict(list)
        for row in cursor.fetchall():
            activities[row["EntityNumber"]].append(row)
            
        cursor.execute(f'SELECT * FROM address WHERE "EntityNumber" IN ({placeholders})', batch_ids)
        addresses = defaultdict(list)
        for row in cursor.fetchall():
            addresses[row["EntityNumber"]].append(row)
            
        cursor.execute(f'SELECT * FROM branch WHERE "EnterpriseNumber" IN ({placeholders})', batch_ids)
        branches = defaultdict(list)
        for row in cursor.fetchall():
            branches[row["EnterpriseNumber"]].append(row)
            
        cursor.execute(f'SELECT * FROM contact WHERE "EntityNumber" IN ({placeholders})', batch_ids)
        contacts = defaultdict(list)
        for row in cursor.fetchall():
            contacts[row["EntityNumber"]].append(row)

        cursor.execute(f'SELECT * FROM establishment WHERE "EnterpriseNumber" IN ({placeholders})', batch_ids)
        establishments = defaultdict(list)
        for row in cursor.fetchall():
            establishments[row["EnterpriseNumber"]].append(row)

        cursor.execute(f'SELECT * FROM denomination WHERE "EntityNumber" IN ({placeholders})', batch_ids)
        denominations = defaultdict(list)
        for row in cursor.fetchall():
            denominations[row["EntityNumber"]].append(row)
            
        bulk_operations = []
        for ent_id, ent_doc in enterprises_data.items():
            primary_key = ent_id.replace(".", "")
            ent_doc["_id"] = primary_key
            
            # Enrichissement des activités avec les descriptions FR/NL
            enriched_activities = []
            for act in activities.get(ent_id, []):
                act_group = act.get("ActivityGroup")
                if act_group and act_group in codes_dict.get("ActivityGroup", {}):
                    act["ActivityGroup_Desc_FR"] = codes_dict["ActivityGroup"][act_group].get("FR")
                    act["ActivityGroup_Desc_NL"] = codes_dict["ActivityGroup"][act_group].get("NL")
                
                clas = act.get("Classification")
                if clas and clas in codes_dict.get("Classification", {}):
                    act["Classification_Desc_FR"] = codes_dict["Classification"][clas].get("FR")
                    act["Classification_Desc_NL"] = codes_dict["Classification"][clas].get("NL")
                
                nace_ver = act.get("NaceVersion", "")
                nace_code = act.get("NaceCode")
                nace_cat = f"Nace{nace_ver}" if nace_ver else "Nace2008"
                if nace_code and nace_code in codes_dict.get(nace_cat, {}):
                    act["NaceCode_Desc_FR"] = codes_dict[nace_cat][nace_code].get("FR")
                    act["NaceCode_Desc_NL"] = codes_dict[nace_cat][nace_code].get("NL")
                
                enriched_activities.append(act)

            ent_doc["activites"] = enriched_activities
            ent_doc["adresses"] = addresses.get(ent_id, [])
            ent_doc["succursales"] = branches.get(ent_id, [])
            ent_doc["contacts"] = contacts.get(ent_id, [])
            ent_doc["etablissements"] = establishments.get(ent_id, [])
            ent_doc["denominations"] = denominations.get(ent_id, [])
            
            bulk_operations.append(
                UpdateOne(
                    {"_id": primary_key},
                    {"$set": ent_doc},
                    upsert=True
                )
            )
            
        if bulk_operations:
            collection.bulk_write(bulk_operations, ordered=False)
            
        if (i + BATCH_SIZE) % 50000 == 0:
            print(f"Progression : {i + BATCH_SIZE} / {total}")

    print("Mise à jour terminée.")
    sqlite_conn.close()
    os.remove(SQLITE_DB)

if __name__ == "__main__":
    codes_dict = load_codes_mapping()
    conn = setup_sqlite()
    process_and_load_mongo(conn, codes_dict)