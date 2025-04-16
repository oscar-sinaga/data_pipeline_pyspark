import pandas as pd
from datetime import datetime


from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target
from src.utils.log import etl_log

def transform_fact_ipos(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform the data from the staging area before loading it into the warehouse area.
    """
    try:
        process = "transformation"

        # columns to picked
        columns_to_picked = [
            'ipo_id', 
            'object_id',
            'public_at',
            'valuation_currency_code',
            'raised_currency_code',
            'valuation_amount',
            'raised_amount',
            'stock_symbol',
            # 'created_at',
            # 'updated_at'
        ]
        data = data.loc[:,columns_to_picked]

        # rename column
        columns_to_renamed = {
            'ipo_id': 'ipo_nk',
            'object_id':'company_nk',
        }
        data = data.rename(columns=columns_to_renamed)

        # deduplication based on relationship_nk
        data = data.drop_duplicates(subset='ipo_nk')

        # fill the nan row data
        # Ubah kolom ke datetime, isi NaT dengan 1900-01-01
        data['public_at'] = pd.to_datetime(data['public_at'], errors='coerce').fillna(pd.Timestamp('2100-01-01'))
        data['valuation_amount'] = data['valuation_amount'].fillna(0)
        data['raised_amount'] = data['raised_amount'].fillna(0)
        data['valuation_currency_code'] = data['valuation_currency_code'].fillna('N/A')
        data['raised_currency_code'] = data['raised_currency_code'].fillna('N/A')
        data['stock_symbol'] = data['stock_symbol'].fillna('N/A')

        # Ubah tipe data
        data['public_at'] = data['public_at'].astype('int')
       
        #Lookup `company_id` from `dim_speciality` table based on `company_nk` 
        dim_company = extract_target('dim_company')
        data['company_id'] = data['company_nk'].apply(lambda x: dim_company.loc[dim_company['company_nk'] == x, 'company_id'].values[0] if len(dim_company.loc[dim_company['company_nk'] == x, 'company_id'].values) > 0 else None)

        # drop unnecessary columns
        columns_dropped = [
            'company_nk'
        ]
        data = data.drop(columns = columns_dropped)

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