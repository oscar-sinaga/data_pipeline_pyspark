from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import csv
from datetime import datetime
from src.utils.helper import startup_investments_engine
import numpy as np

def extract_list_table():
    try:
        # create connection to database
        conn =  startup_investments_engine()
        
        # Get list of tables in the database
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        df = pd.read_sql(sql=query, con=conn)
        return df
    except Exception as e:
        print(e)
        return None

def extract_database(table_name: str): 
    try:
        # create connection to database
        conn =  startup_investments_engine()

        # Constructs a SQL query to select all columns from the specified table_name
        query = f"SELECT * FROM {table_name}"

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn)

        # Replace all data which is '' to be np.nan
        df = df.replace('', np.nan)

        return df
    except Exception as e:
        print(e)
        return None

    