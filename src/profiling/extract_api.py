import pandas as pd
from dotenv import load_dotenv
import requests
from datetime import datetime
import numpy as np

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