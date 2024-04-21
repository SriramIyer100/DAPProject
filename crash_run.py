from dagster import job
from extract import *
from transform_and_load import *

@job
def etl():
    load(
        transform_crash(
            extract_crash()
                    )
        )