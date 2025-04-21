import pandas as pd
from datetime import datetime
from src.utils.helper import stg_engine
from src.utils.log import etl_log, read_etl_log
import numpy as np

def extract_database(table_name: str): 
    try:
        # create connection to database
        conn = stg_engine()

        # Get date from previous process
        filter_log = {"step_name": "warehouse",
                    "table_name": table_name,
                    "status": "success",
                    "process": "load"}
        etl_date = read_etl_log(filter_log)


        # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
        # Otherwise, retrieve data added since the last successful extraction (etl_date).
        if(etl_date['max'][0] == None):
            etl_date = '1111-01-01'
        else:
            etl_date = etl_date[max][0]
        
        query = f"SELECT * FROM {table_name} WHERE created_at  > %s::timestamp"

        #Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn, params=(etl_date,))
        df = df.replace('', np.nan)
        log_msg = {
                "step" : "warehouse",
                "process":"extraction",
                "status": "success",
                "source": "database",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return df
    except Exception as e:
        print(e)
        log_msg = {
            "step" : "warehouse",
            "process":"extraction",
            "status": "failed",
            "source": "database",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
    finally:
        etl_log(log_msg)

    

    