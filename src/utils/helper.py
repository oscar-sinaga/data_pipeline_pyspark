from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
# import sentry_sdk
from pathlib import Path

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
MINIO_CONSOLE_PORT=os.getenv("MINIO_CONSOLE_PORT")
MINIO_ROOT_USER=os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD=os.getenv("MINIO_ROOT_PASSWORD")
ACCESS_KEY_MINIO = os.getenv("ACCESS_KEY_MINIO")
SECRET_KEY_MINIO = os.getenv("SECRET_KEY_MINIO")
PROFILING_BUCKET_NAME = os.getenv("PROFILING_BUCKET_NAME")


DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_PASS = os.getenv("DB_PASS")

DB_NAME_STARTUP_INVESTMENTS = os.getenv("DB_NAME_STARTUP_INVESTMENTS")

def startup_investments_engine():
    return create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME_STARTUP_INVESTMENTS}")
