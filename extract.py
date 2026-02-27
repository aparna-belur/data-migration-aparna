import pandas as pd
from pymongo import MongoClient
from config import MONGO_CONFIG


#MongoDB connection
client = MongoClient(MONGO_CONFIG["uri"])
db = client[MONGO_CONFIG["database"]]

#extraction 
def extract(collection_name):
 

    collection = db[collection_name]

    data = list(collection.find({}, {"_id": 0}))

    if not data:
        raise ValueError(f"No data found in collection: {collection_name}")
    
    df = pd.DataFrame(data)
    return df

    