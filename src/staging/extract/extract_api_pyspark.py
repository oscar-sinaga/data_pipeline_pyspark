import pandas as pd
from dotenv import load_dotenv
import requests
from datetime import datetime
import numpy as np
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.utils.log import etl_log, read_etl_log
from datetime import timedelta


LINK_API_MILESTONE = "https://api-milestones.vercel.app/api/data"

link_api = LINK_API_MILESTONE

def extract_api(link_api:str, list_parameter:dict):
    try:
        # Establish connection to API
        resp = requests.get(link_api, params=list_parameter)

        # Parse the response JSON
        raw_response = resp.json()

        # Convert the JSON data to a pandas DataFrame
        df_api = pd.DataFrame(raw_response)

        # Replace all data which is '' to be np.nan
        df_api = df_api.replace('', np.nan)

        return df_api

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the API request: {e}")
    

    except ValueError as e:
        print(f"An error occurred while parsing the response JSON: {e}")

def extract_backfilling(start_date, current_date):
    df_milestones = pd.DataFrame()

    # Pastikan string format
    if not isinstance(start_date, str):
        start_date = start_date.strftime("%Y-%m-%d")
    if not isinstance(current_date, str):
        current_date = current_date.strftime("%Y-%m-%d")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(current_date, "%Y-%m-%d")

    # Handle kondisi start_date == current_date
    if start == end:
        list_parameter = {
            "start_date": start_date,
            "end_date": (end + timedelta(days=1)).strftime("%Y-%m-%d")  # Buat end_date jadi satu hari setelahnya
        }
        df_backfilling = extract_api(link_api, list_parameter)
        return df_backfilling.drop_duplicates(keep='first')

    # Jika selisih tidak lebih dari 1 tahun, langsung ambil semua
    if end - start <= timedelta(days=365):
        list_parameter = {
            "start_date": start_date,
            "end_date": current_date
        }
        df_backfilling = extract_api(link_api, list_parameter)
        return df_backfilling.drop_duplicates(keep='first')

    # Jika lebih dari 1 tahun, lakukan loop tahunan
    while start + relativedelta(years=1) < end:
        temp_end = start + relativedelta(years=1)
        list_parameter = {
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": temp_end.strftime("%Y-%m-%d")
        }
        df_backfilling = extract_api(link_api, list_parameter)
        df_milestones = pd.concat([df_milestones, df_backfilling])
        df_milestones = df_milestones.drop_duplicates(keep='first')
        start = temp_end

    # Ambil sisa data dari akhir loop sampai current_date
    list_parameter = {
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": current_date
    }
    df_backfilling = extract_api(link_api, list_parameter)
    df_milestones = pd.concat([df_milestones, df_backfilling])
    df_milestones = df_milestones.drop_duplicates(keep='first')

    return df_milestones



# Extract data from 1950-01-01 in chunks of 1 year until today
def extract_api_milestones(table_name:str):
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        # link_api = "https://api-milestones.vercel.app/api/data"

        # Get date from previous process
        filter_log = {"step_name": "staging",
                    "table_name": table_name,
                    "status": "success",
                    "process": "load"}
        etl_date = read_etl_log(filter_log)

        # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
            # Otherwise, retrieve data added since the last successful extraction (etl_date).
        if(etl_date['max'][0] == None):
            etl_date = "1950-01-01"
        else:
            etl_date = etl_date[max][0]
            # etl_date = etl_date.strftime("%Y-%m-%d")

        df = extract_backfilling(etl_date,current_date)
        etl_date = pd.to_datetime(etl_date)      # Pastikan dalam format datetime
        df['created_at'] = pd.to_datetime(df['created_at'])
        df = df[df['created_at'] > etl_date]

        log_msg = {
                "step" : "staging",
                "process":"extraction",
                "status": "success",
                "source": "api",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return df
    except Exception as e:
        print(e)
        log_msg = {
            "step" : "staging",
            "process":"extraction",
            "status": "failed",
            "source": "api",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
    finally:
        etl_log(log_msg)


