import pandas as pd
from datetime import datetime


from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target
from src.utils.log import etl_log

def transform_fact_acquisitions(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform the data from the staging area before loading it into the warehouse area.
    """
    try:
        process = "transformation"

        # columns to picked
        columns_to_picked = [
            'acquisition_id', 
            'acquiring_object_id',
            'acquired_object_id',
            'acquired_at',
            'price_amount',
            'price_currency_code',
            'term_code',
            # 'created_at',
            # 'updated_at'
        ]
        data = data.loc[:,columns_to_picked]

        # rename column
        columns_to_renamed = {
            'acquisition_id': 'acquisition_nk',
            'acquiring_object_id':'acquiring_company_id',
            'acquired_object_id':'acquired_company_id',
        }
        data = data.rename(columns=columns_to_renamed)  
        
        # deduplication based on relationship_nk
        data = data.drop_duplicates(subset='acquisition_nk')

        # fill the nan row data
        # Ubah kolom ke datetime, isi NaT dengan 1900-01-01
        data['acquired_at'] = pd.to_datetime(data['acquired_at'], errors='coerce').fillna(pd.Timestamp('2100-01-01'))
        data['term_code'] = data['term_code'].fillna('Unknown')
        data['price_currency_code'] = data['price_currency_code'].fillna('N/A')
        data['price_amount'] = data['price_amount'].fillna(0)

        # Ubah tipe data
        data['acquired_at'] = data['acquired_at'].astype('int')

        #Lookup `people_id` from `dim_company` table based on `company_nk` 
        company = extract_target('dim_company')
        data['people_id'] = data['people_nk'].apply(lambda x: company.loc[company['people_nk'] == x, 'people_id'].values[0] if len(company.loc[company['people_nk'] == x, 'people_id'].values) > 0 else None)
       
        #Lookup `company_id` from `dim_speciality` table based on `company_nk` 
        company = extract_target('dim_company')
        data['company_id'] = data['company_nk'].apply(lambda x: company.loc[company['company_nk'] == x, 'company_id'].values[0] if len(company.loc[company['company_nk'] == x, 'company_id'].values) > 0 else None)

        # drop unnecessary columns
        columns_dropped = [
            'people_id',
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