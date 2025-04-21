import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import os
from pangres import upsert
from datetime import datetime

from src.utils.helper import wh_engine
from src.staging.load.handle_error import handle_error
from src.utils.log import etl_log


def load_warehouse(data, schema:str, table_name: str, idx_name:str, source, table_process:str):
    try:
        # create connection to database
        conn = wh_engine()
        
        # set data index or primary key
        data = data.set_index(idx_name)
        
        # Do upsert (Update for existing data and Insert for new data)
        upsert(con = conn,
                df = data,
                table_name = table_name,
                schema = schema,
                if_row_exists = "update")
        
        #create success log message
        log_msg = {
                "step" : "warehouse",
                "process":"load",
                "status": "success",
                "source": source,
                "table_name": table_process,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        # return data
    except Exception as e:

        #create fail log message
        log_msg = {
            "step" : "warehouse",
            "process":"load",
            "status": "failed",
            "source": source,
            "table_name": table_process,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S") , # Current timestamp
            "error_msg": str(e)
        }
        print(e)
        # Handling error: save data to Object Storage
        try:
            handle_error(data = data, table_name= table_name, process='warehouse_load')
        except Exception as e:
            print(e)
    finally:
        etl_log(log_msg)

    