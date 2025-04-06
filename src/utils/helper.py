from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
# import sentry_sdk
from pathlib import Path

# Load .env dari root
BASE_DIR = Path(__file__).resolve().parents[2]  # Mengarah ke folder data_pipeline_pyspark/
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Build path absolut dari file kredensial
cred_path = os.getenv("CRED_PATH")
CRED_PATH = str(BASE_DIR / cred_path)  # Ini jadi path absolut

KEY_SPREADSHEET_PEOPLE = os.getenv("KEY_SPREADSHEET_PEOPLE")
KEY_SPREADSHEET_RELATIONSHIPS = os.getenv("KEY_SPREADSHEET_RELATIONSHIPS")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_PASS = os.getenv("DB_PASS")

DB_NAME_STARTUP_INVESTMENTS = os.getenv("DB_NAME_STARTUP_INVESTMENTS")

def startup_investments_engine():
    return create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME_STARTUP_INVESTMENTS}")
