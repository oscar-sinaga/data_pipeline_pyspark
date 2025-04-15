from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
# import sentry_sdk
from pathlib import Path
import pandas as pd

# Load .env dari root
BASE_DIR = Path(__file__).resolve().parents[2]  # Mengarah ke folder data_pipeline_pyspark/
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Build path absolut dari file kredensial spreadsheet
cred_path = os.getenv("CRED_PATH")
CRED_PATH = str(BASE_DIR / cred_path)  # Ini jadi path absolut

KEY_SPREADSHEET_PEOPLE = os.getenv("KEY_SPREADSHEET_PEOPLE")
KEY_SPREADSHEET_RELATIONSHIPS = os.getenv("KEY_SPREADSHEET_RELATIONSHIPS")

#Minio
MINIO_PORT=os.getenv("MINIO_PORT")
MINIO_HOST=os.getenv("MINIO_HOST")

MINIO_CONSOLE_PORT=os.getenv("MINIO_CONSOLE_PORT")
MINIO_ROOT_USER=os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD=os.getenv("MINIO_ROOT_PASSWORD")
ACCESS_KEY_MINIO = os.getenv("ACCESS_KEY_MINIO")
SECRET_KEY_MINIO = os.getenv("SECRET_KEY_MINIO")
PROFILING_BUCKET_NAME = os.getenv("PROFILING_BUCKET_NAME")
# ERROR_STAGING_SI_BUCKET_NAME = os.getenv("ERROR_STAGING_SI_BUCKET_NAME")


DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_PASS = os.getenv("DB_PASS")

DB_NAME_STARTUP_INVESTMENTS = os.getenv("DB_NAME_STARTUP_INVESTMENTS")
# DB_NAME_STG = os.getenv("DB_NAME_STG")
# DB_NAME_LOG = os.getenv("DB_NAME_LOG")
# DB_NAME_WH = os.getenv("DB_NAME_WH")
# DB_PORT_WH = os.getenv("DB_PORT_WH")

# #MODEL PATH
# MODEL_PATH_LOG_ETL = str(BASE_DIR / os.getenv("MODEL_PATH_LOG_ETL"))


def startup_investments_engine():
    return create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME_STARTUP_INVESTMENTS}")

# def stg_engine():
#     return create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT_WH}/{DB_NAME_STG}")

# def log_engine():
#     return create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT_WH}/{DB_NAME_LOG}")

# def wh_engine():
#     return create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT_WH}/{DB_NAME_WH}")

# def read_sql(PATH, table_name):
#     #open your file .sql
#     with open(f"{PATH}{table_name}.sql", 'r') as file:
#         content = file.read()
    
#     #return query text
#     return content

# def extract_target(table_name: str):
#     """
#     this function is used to extract data from the data warehouse.
#     """
#     conn = wh_engine()

#     # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
#     query = f"SELECT * FROM {table_name}"

#     # Execute the query with pd.read_sql
#     df = pd.read_sql(sql=query, con=conn)
    
#     return df