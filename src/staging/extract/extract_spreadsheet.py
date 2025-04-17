from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from dotenv import load_dotenv
import os
import csv
from datetime import datetime
import numpy as np 
from pathlib import Path
from src.utils.helper import CRED_PATH,KEY_SPREADSHEET_PEOPLE,KEY_SPREADSHEET_RELATIONSHIPS
from src.utils.log import etl_log

def auth_gspread():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    #Define your credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CRED_PATH, scope) # Your json file here
    # print("Connected as:", credentials.service_account_email)
    
    gc = gspread.authorize(credentials)

    return gc

def init_key_file(table_name:str):
    #define credentials to open the file
    gc = auth_gspread()
    
    #open spreadsheet file by key
    table_name = table_name.upper()
    if table_name=='PEOPLE':
        sheet_result = gc.open_by_key(KEY_SPREADSHEET_PEOPLE)
    elif table_name == 'RELATIONSHIPS':
        sheet_result = gc.open_by_key(KEY_SPREADSHEET_RELATIONSHIPS)
    else:
        raise ValueError(f"Invalid table name '{table_name}'. Expected 'PEOPLE' or 'RELATIONSHIPS'.")

    return sheet_result

def extract_sheet(table_name:str) -> pd.DataFrame:
    # init sheet
    sheet_result = init_key_file(table_name)
    
    worksheet_result = sheet_result.get_worksheet(0)
    
    df_result = pd.DataFrame(worksheet_result.get_all_values())
    
    # set first rows as columns
    df_result.columns = df_result.iloc[0]
    
    # get all the rest of the values
    df_result = df_result[1:].copy()

    # Replace all data which is '' to be np.nan
    df_result = df_result.replace('', np.nan)
    
    return df_result

def extract_spreadsheet(table_name: str):

    try:
        # extract data
        df_data = extract_sheet(table_name = table_name)
        
        # success log message
        log_msg = {
            "step" : "staging",
            "status": "success",
            "source": "spreadsheet",
            "table_name": table_name,
            "process": "extraction",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
        return df_data
    except Exception as e:
        # fail log message
        log_msg = {
            "step" : "staging",
            "status": "failed",
            "source": "spreadsheet",
            "process": "extraction",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
    finally:
        # load log to csv file
       etl_log(log_msg)
        
