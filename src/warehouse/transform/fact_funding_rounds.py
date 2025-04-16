import pandas as pd
from datetime import datetime


from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target
from src.utils.log import etl_log

def transform_fact_funding_rounds(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform the data from the staging area before loading it into the warehouse area.
    """
    try:
        process = "transformation"

        # columns to picked
        columns_to_picked = [
            'funding_round_id', 
            'object_id',
            'funded_at',
            'funding_round_type',
            'funding_round_code',
            'raised_amount_usd',
            'pre_money_valuation_usd',
            'post_money_valuation_usd',
            'is_first_round',
            'is_last_round',
            # 'created_at',
            # 'updated_at'
        ]
        data = data.loc[:,columns_to_picked]

        # rename column
        columns_to_renamed = {
            'funding_round_id': 'funding_round_nk',
            'object_id':'company_nk',
            'is_first_round':'round_position_desc',
            'is_last_round':'round_stage_desc'
        }
        data = data.rename(columns=columns_to_renamed)

        # Mapping nilai boolean ke deskripsi
        data['round_position_desc'] = data['round_position_desc'].map({
            True: 'First Round',
            False: 'Not First Round'
        })

        data['round_stage_desc'] = data['round_stage_desc'].map({
            True: 'Last Round',
            False: 'Ongoing Round'
        })  
        
        # deduplication based on relationship_nk
        data = data.drop_duplicates(subset='funding_round_nk')

        # fill the nan row data
        # Ubah kolom ke datetime, isi NaT dengan 1900-01-01
        data['funded_at'] = pd.to_datetime(data['funded_at'], errors='coerce').fillna(pd.Timestamp('2100-01-01'))
        data['raised_amount_usd'] = data['raised_amount_usd'].fillna(0)
        data['pre_money_valuation_usd'] = data['pre_money_valuation_usd'].fillna(0)
        data['post_money_valuation_usd'] = data['post_money_valuation_usd'].fillna(0)

        # Ubah tipe data
        data['funded_at'] = data['funded_at'].astype('int')
       
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