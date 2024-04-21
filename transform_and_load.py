import numpy as np
import pandas as pd
from dagster import op, Out, In, get_dagster_logger
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from pymongo import MongoClient, errors
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
from sqlalchemy.types import *


mongo_connection_string = "mongodb://127.0.0.1:27017"
postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/crashdb"
logger = get_dagster_logger()

CrashDataFrame = create_dagster_pandas_dataframe_type(
    name="CrashDataFrame",
    columns=[
        PandasColumn.string_column(name="CRASH_RECORD_ID", non_nullable=True),
        PandasColumn.string_column(name="CRASH_DATE_EST_I", non_nullable=True),
        PandasColumn.string_column(name="CRASH_DATE", non_nullable=True),
        PandasColumn.integer_column(name="POSTED_SPEED_LIMIT", non_nullable=True),
        PandasColumn.string_column(name="TRAFFIC_CONTROL_DEVICE", non_nullable=True),
        PandasColumn.string_column(name="DEVICE_CONDITION", non_nullable=True),
        PandasColumn.string_column(name="WEATHER_CONDITION", non_nullable=True),
        PandasColumn.string_column(name="LIGHTING_CONDITION", non_nullable=True),
        PandasColumn.string_column(name="FIRST_CRASH_TYPE", non_nullable=True),
        PandasColumn.string_column(name="TRAFFICWAY_TYPE", non_nullable=True),
        PandasColumn.integer_column(name="LANE_CNT", non_nullable=True),
        PandasColumn.string_column(name="ALIGNMENT", non_nullable=True),
        PandasColumn.string_column(name="ROADWAY_SURFACE_COND", non_nullable=True),
        PandasColumn.string_column(name="ROAD_DEFECT", non_nullable=True),
        PandasColumn.string_column(name="REPORT_TYPE", non_nullable=True),
        PandasColumn.string_column(name="CRASH_TYPE", non_nullable=True),
        PandasColumn.string_column(name="INTERSECTION_RELATED_I", non_nullable=True),
        PandasColumn.string_column(name="NOT_RIGHT_OF_WAY_I", non_nullable=True),
        PandasColumn.string_column(name="HIT_AND_RUN_I", non_nullable=True),
        PandasColumn.string_column(name="DAMAGE", non_nullable=True),
        PandasColumn.string_column(name="DATE_POLICE_NOTIFIED", non_nullable=True),
        PandasColumn.string_column(name="PRIM_CONTRIBUTORY_CAUSE", non_nullable=True),
        PandasColumn.string_column(name="SEC_CONTRIBUTORY_CAUSE", non_nullable=True),
        PandasColumn.integer_column(name="STREET_NO", non_nullable=True),
        PandasColumn.string_column(name="STREET_DIRECTION", non_nullable=True),
        PandasColumn.string_column(name="STREET_NAME", non_nullable=True),
        PandasColumn.integer_column(name="BEAT_OF_OCCURRENCE", non_nullable=True),
        PandasColumn.integer_column(name="NUM_UNITS", non_nullable=True),
        PandasColumn.string_column(name="MOST_SEVERE_INJURY", non_nullable=True),
        PandasColumn.integer_column(name="INJURIES_TOTAL", non_nullable=True),
        PandasColumn.integer_column(name="INJURIES_FATAL", non_nullable=True),
        PandasColumn.integer_column(name="INJURIES_INCAPACITATING", non_nullable=True),
        PandasColumn.integer_column(name="INJURIES_NON_INCAPACITATING", non_nullable=True),
        PandasColumn.integer_column(name="INJURIES_REPORTED_NOT_EVIDENT", non_nullable=True),
        PandasColumn.integer_column(name="INJURIES_NO_INDICATION", non_nullable=True),
        PandasColumn.integer_column(name="INJURIES_UNKNOWN", non_nullable=True),
        PandasColumn.integer_column(name="CRASH_HOUR", non_nullable=True),
        PandasColumn.integer_column(name="CRASH_DAY_OF_WEEK", non_nullable=True),
        PandasColumn.integer_column(name="CRASH_MONTH", non_nullable=True),
        PandasColumn.float_column(name="LATITUDE", non_nullable=True),
        PandasColumn.float_column(name="LONGITUDE", non_nullable=True),
        PandasColumn.string_column(name="LOCATION", non_nullable=True)
    ]
)


@op(
    ins={"start": In(bool)},
    out=Out(pd.DataFrame)
)



def transform_crash(start):
    
    client = MongoClient(mongo_connection_string)    
    data = client["Project_2"]
    collect = data["C1"]
    
    query = {}
    projection = {"_id": 0}
    data = list(collect.find(query, projection))
    crash_df = pd.DataFrame(data)
    
    
    return crash_df
    
@op(
    ins={"crash_df": In(pd.DataFrame)},
    out=Out(pd.DataFrame)
)

def load(crash_df):
    try:
        engine = create_engine(postgres_connection_string,poolclass=NullPool)
                
        database_datatypes = dict(
            zip(crash_df.columns,[VARCHAR]*len(crash_df.columns))
        )
        
        with engine.connect() as conn:
            rowcount = crash_df.to_sql(
                name="crashtable",
                schema="public",
                dtype=database_datatypes,
                con=engine,
                index=False,
                if_exists="replace"
            )
            logger.info("{} records loaded".format(rowcount))
            
        engine.dispose(close=True)        
        
        return crash_df
        #return None
        #return rowcount > 0
    
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False
    
