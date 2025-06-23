import pandas as pd
from datetime import datetime


from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target
from src.utils.log import etl_log

def transform_fact_investments(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform the data from the staging area before loading it into the warehouse area.
    """
    try:
        process = "transformation"

        # columns to picked
        columns_to_picked = [
            'investment_id', 
            'funding_round_id',
            'funded_object_id',
            'investor_object_id',
            # 'created_at',
            # 'updated_at'
        ]
        data = data.loc[:,columns_to_picked]

        # rename column
        columns_to_renamed = {
            'investment_id': 'investment_nk',
            'funding_round_id':'funding_round_nk',
            'funded_object_id':'investee_company_nk',
            'investor_object_id':'investor_company_nk',

        }
        data = data.rename(columns=columns_to_renamed)  
        
        # deduplication based on relationship_nk
        data = data.drop_duplicates(subset='investment_id')

        # Ubah tipe data
        data['investment_nk'] = data['investment_nk'].astype('int')
        data['funding_round_nk'] = data['funding_round_nk'].astype('int')

        fact_funding_rounds = extract_target('fact_funding_rounds')
        # Lookup funding_round_id
        data['funding_round_id'] = data['funding_round_nk'].apply(
            lambda x: fact_funding_rounds.loc[fact_funding_rounds['funding_round_nk'] == x, 'funding_round_id'].values[0]
            if len(fact_funding_rounds.loc[fact_funding_rounds['funding_round_nk'] == x, 'funding_round_id'].values) > 0 else None
        )

        dim_company = extract_target('dim_company')
        # Lookup investee_company_id
        data['investee_company_id'] = data['investee_company_nk'].apply(
            lambda x: dim_company.loc[dim_company['company_nk'] == x, 'company_id'].values[0]
            if len(dim_company.loc[dim_company['company_nk'] == x, 'company_id'].values) > 0 else None
        )
        
        # Lookup investor_company_id
        data['investor_company_id'] = data['investor_company_nk'].apply(
            lambda x: dim_company.loc[dim_company['company_nk'] == x, 'company_id'].values[0]
            if len(dim_company.loc[dim_company['company_nk'] == x, 'company_id'].values) > 0 else None
        )
        
        # Drop natural key columns
        columns_to_drop = ['funding_round_nk',
                           'investee_company_nk', 
                           'investor_company_nk']
        data = data.drop(columns=columns_to_drop)

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