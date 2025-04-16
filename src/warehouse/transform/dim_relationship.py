import pandas as pd
from datetime import datetime


from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target
from src.utils.log import etl_log

def transform_dim_relationship(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    This function is used to transform the data from the staging area before loading it into the warehouse area.
    """
    try:
        process = "transformation"

        # columns to picked
        columns_to_picked = [
            'relationship_id', 
            'person_object_id',
            'relationship_object_id',
            'title',
            'start_at',
            'end_at',
            'is_past',
            'sequence',
            # 'created_at',
            # 'updated_at'
        ]
        data = data.loc[:,columns_to_picked]

        # rename column
        columns_to_renamed = {
            'relationship_id': 'relationship_nk',
            'person_object_id':'people_nk',
            'relationship_object_id':'company_nk',
            'is_past':'relationship_status',
            'sequence' : 'relationship_order'
        }
        data = data.rename(columns=columns_to_renamed)  
        
        # deduplication based on relationship_nk
        data = data.drop_duplicates(subset='relationship_nk')

        # fill the nan row data
        # Ubah kolom ke datetime, isi NaT dengan 1900-01-01
        data['start_at'] = pd.to_datetime(data['start_at'], errors='coerce').fillna(pd.Timestamp('2100-01-01'))
        data['end_at'] = pd.to_datetime(data['end_at'], errors='coerce').fillna(pd.Timestamp('2100-01-01'))
        data['title'] = data['title'].fillna('Unknown')
        data['relationship_status'] = data['relationship_status'].fillna('Unknown')
        data['relationship_order'] = data['relationship_order'].fillna(0)

        # Ubah tipe data
        data['start_at'] = data['start_at'].astype('int')
        data['end_at'] = data['end_at'].astype('int')
        data['relationship_nk'] = data['relationship_nk'].astype('int')
        data['relationship_order'] = data['relationship_order'].astype('int')

        #Lookup `people_id` from `dim_speciality` table based on `company_nk` 
        dim_people = extract_target('dim_people')
        data['people_id'] = data['people_nk'].apply(lambda x: dim_people.loc[dim_people['people_nk'] == x, 'people_id'].values[0] if len(dim_people.loc[dim_people['people_nk'] == x, 'people_id'].values) > 0 else None)
       
        #Lookup `company_id` from `dim_speciality` table based on `company_nk` 
        dim_company = extract_target('dim_company')
        data['company_id'] = data['company_nk'].apply(lambda x: dim_company.loc[dim_company['company_nk'] == x, 'company_id'].values[0] if len(dim_company.loc[dim_company['company_nk'] == x, 'company_id'].values) > 0 else None)

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