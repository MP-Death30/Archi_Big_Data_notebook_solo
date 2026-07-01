import os
import pandas as pd
from pymongo import MongoClient, UpdateOne

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "kbo_db" 
COL_SOURCE = "enterprises_rich"
COL_TARGET = "enterprise_silver"

def parse_date(field_ref):
    return {
        "$cond": {
            "if": {"$regexMatch": {"input": {"$ifNull": [field_ref, ""]}, "regex": r"^\d{2}-\d{2}-\d{4}$"}},
            "then": {"$dateFromString": {"dateString": field_ref, "format": "%d-%m-%Y", "onError": field_ref}},
            "else": {
                "$cond": {
                    "if": {"$regexMatch": {"input": {"$ifNull": [field_ref, ""]}, "regex": r"^\d{4}-\d{2}-\d{2}.*"}},
                    "then": {"$dateFromString": {"dateString": {"$substr": [field_ref, 0, 10]}, "format": "%Y-%m-%d", "onError": field_ref}},
                    "else": field_ref
                }
            }
        }
    }

def process_silver_layer():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    codes_csv_path = os.path.join(base_dir, "data", "KBO", "code.csv")

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col_bronze = db[COL_SOURCE]
    col_silver = db[COL_TARGET]

    count_bronze = col_bronze.count_documents({})
    print(f"Documents source : {count_bronze}")
    if count_bronze == 0:
        raise ValueError("Arrêt : Collection source vide.")

    print("Exécution du pipeline d'agrégation Silver (Casting Dates BSON + Purge _NL)...")
    
    pipeline = [
        {
            "$set": {
                "StartDate": parse_date("$StartDate"),
                "adresses": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": {"$ifNull": ["$adresses", []]},
                                "as": "addr",
                                "cond": {"$eq": ["$$addr.TypeOfAddress", "REGO"]}
                            }
                        },
                        "as": "addr",
                        "in": {"$mergeObjects": ["$$addr", {"StartDate": parse_date("$$addr.StartDate")}]}
                    }
                },
                "etablissements": {
                    "$map": {
                        "input": {"$ifNull": ["$etablissements", []]},
                        "as": "etab",
                        "in": {"$mergeObjects": ["$$etab", {"StartDate": parse_date("$$etab.StartDate")}]}
                    }
                },
                "activites": {
                    "$map": {
                        "input": {"$ifNull": ["$activites", []]},
                        "as": "act",
                        "in": {"$mergeObjects": ["$$act", {"StartDate": parse_date("$$act.StartDate")}]}
                    }
                },
                "denominations": {
                    "$map": {
                        "input": {
                            "$sortArray": {
                                "input": {"$ifNull": ["$denominations", []]},
                                "sortBy": {"TypeOfDenomination": 1}
                            }
                        },
                        "as": "denom",
                        "in": {"$mergeObjects": ["$$denom", {"StartDate": parse_date("$$denom.StartDate")}]}
                    }
                },
                "succursales": {
                    "$map": {
                        "input": {"$ifNull": ["$succursales", []]},
                        "as": "succ",
                        "in": {"$mergeObjects": ["$$succ", {"StartDate": parse_date("$$succ.StartDate")}]}
                    }
                }
            }
        },
        {
            "$set": {
                "activites": {
                    "$reduce": {
                        "input": "$activites",
                        "initialValue": [],
                        "in": {
                            "$cond": [
                                {"$in": [
                                    {"$concat": [{"$toString": "$$this.NaceCode"}, "_", "$$this.Classification"]},
                                    {"$map": {
                                        "input": "$$value",
                                        "as": "v",
                                        "in": {"$concat": [{"$toString": "$$v.NaceCode"}, "_", "$$v.Classification"]}
                                    }}
                                ]},
                                "$$value",
                                {"$concatArrays": ["$$value", ["$$this"]]}
                            ]
                        }
                    }
                }
            }
        },
        {
            "$unset": [
                "Language_NL",
                "JuridicalForm_Desc_NL",
                "Status_Desc_NL",
                "TypeOfEnterprise_Desc_NL",
                "activites.ActivityGroup_Desc_NL",
                "activites.Classification_Desc_NL",
                "activites.NaceCode_Desc_NL",
                "adresses.TypeOfAddress_Desc_NL",
                "adresses.MunicipalityNL",
                "adresses.StreetNL",
                "adresses.CountryNL",
                "adresses.ExtraAddressInfoNL",
                "denominations.TypeOfDenomination_Desc_NL",
                "denominations.Language_Desc_NL",
                "etablissements.Language_Desc_NL",
                "succursales.Language_Desc_NL",
                "contacts.EntityContact_Desc_NL"
            ]
        },
        {"$out": COL_TARGET}
    ]

    col_bronze.aggregate(pipeline)
    count_silver = col_silver.count_documents({})
    print(f"Documents transférés (Silver) : {count_silver}")

    if not os.path.exists(codes_csv_path):
        raise FileNotFoundError(f"Fichier code.csv introuvable : {codes_csv_path}")

    print("Application des Labels statiques...")
    df_codes = pd.read_csv(codes_csv_path, dtype=str)
    df_codes_fr = df_codes[df_codes['Language'] == 'FR']

    map_juridical = dict(zip(
        df_codes_fr[df_codes_fr['Category'] == 'JuridicalForm']['Code'],
        df_codes_fr[df_codes_fr['Category'] == 'JuridicalForm']['Description']
    ))
    map_status = dict(zip(
        df_codes_fr[df_codes_fr['Category'] == 'Status']['Code'],
        df_codes_fr[df_codes_fr['Category'] == 'Status']['Description']
    ))
    map_nace = dict(zip(
        df_codes_fr[df_codes_fr['Category'].str.startswith('Nace')]['Code'],
        df_codes_fr[df_codes_fr['Category'].str.startswith('Nace')]['Description']
    ))

    cursor = col_silver.find({})
    bulk_ops = []
    processed = 0

    for doc in cursor:
        update_fields = {}
        
        if "JuridicalForm" in doc:
            update_fields["JuridicalFormLabel"] = map_juridical.get(str(doc["JuridicalForm"]))
            
        if "Status" in doc:
            update_fields["StatusLabel"] = map_status.get(str(doc["Status"]))
            
        if "activites" in doc and isinstance(doc["activites"], list):
            updated_activities = []
            for act in doc["activites"]:
                act_copy = act.copy()
                code_nace = str(act_copy.get("NaceCode", ""))
                act_copy["NaceLabel"] = map_nace.get(code_nace)
                updated_activities.append(act_copy)
            update_fields["activites"] = updated_activities
            
        if update_fields:
            bulk_ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": update_fields}))
            
        if len(bulk_ops) >= 10000:
            col_silver.bulk_write(bulk_ops)
            processed += len(bulk_ops)
            print(f"Labels injectés : {processed}/{count_silver}")
            bulk_ops = []

    if bulk_ops:
        col_silver.bulk_write(bulk_ops)
        processed += len(bulk_ops)
        print(f"Labels injectés : {processed}/{count_silver}")

    print("Architecture Silver validée.")

if __name__ == "__main__":
    process_silver_layer()