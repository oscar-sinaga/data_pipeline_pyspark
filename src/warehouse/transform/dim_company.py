import pandas as pd
from datetime import datetime


from src.warehouse.load.handle_error import handle_error
# from src.utils.helper import extract_target
from src.utils.log import etl_log

def transform_dim_company(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform the data from the staging area before loading it into the warehouse area.
    """
    try:
        process = "transformation"

        # columns to picked
        columns_to_picked = [
            'object_id', 
            'description',
            'region',
            'city',
            'state_code',
            'country_code',
            'latitude',
            'longitude',
            # 'created_at',
            # 'updated_at'
        ]
        data = data.loc[:,columns_to_picked]

        # rename column
        columns_to_renamed = {
            'object_id': 'company_nk'
        }
        data = data.rename(columns=columns_to_renamed)  
        
        # deduplication based on company_nk
        data = data.drop_duplicates(subset='company_nk')

        # fill the nan row data
        data['description'] = data['description'].fillna('No Description')
        data['region'] = data['region'].fillna('Unknown')
        data['city'] = data['city'].fillna('Unknown')
        data['state_code'] = data['state_code'].fillna('N/A')
        data['country_code'] = data['country_code'].fillna('N/A')
        data['latitude'] = data['latitude'].fillna(999)
        data['longitude'] = data['longitude'].fillna(999)

        log_msg = {
                "step" : "warehouse",
                "process": process,
                "status": "success",
                "source": "staging",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        
        return data
    except Exception as e:
        print(e)
        log_msg = {
            "step" : "warehouse",
            "process": process,
            "status": "failed",
            "source": "staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }
        
         # Handling error: save data to Object Storage
        try:
            handle_error(data = data, table_name= table_name, process=process)
        except Exception as e:
            print(e)
    finally:
        # Save the log message
        etl_log(log_msg)