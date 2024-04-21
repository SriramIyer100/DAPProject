


import json
import pandas as pd
from pymongo import MongoClient
from dagster import op, Out, get_dagster_logger

mongo_connection_string = "mongodb://127.0.0.1:27017"
logger = get_dagster_logger()


@op(out=Out(bool))
def extract_crash() -> bool:
    result = False
    try:
        df = pd.read_csv(
            "Traffic_Crashes_-_Crashes.csv",
            memory_map=True,
            low_memory=False
        )
    
        
        client = MongoClient(mongo_connection_string)
        
        data = client["Project_2"]
        
        collect = data["C1"]
        
        crash_json = json.loads(df.to_json(orient="records"))
        collect.insert_many(crash_json)
        
        result = True
    
    except Exception as err:
        logger.error("Error: %s" % err)
    
    return result

