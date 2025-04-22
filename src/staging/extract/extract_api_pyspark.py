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
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame


LINK_API_MILESTONE = "https://api-milestones.vercel.app/api/data"

link_api = LINK_API_MILESTONE


def extract_api_spark(spark: SparkSession, link_api: str, list_parameter: dict):
    try:
        # Request ke API
        resp = requests.get(link_api, params=list_parameter)
        raw_response = resp.json()

        # Ubah ke Spark DataFrame
        df_pandas = pd.DataFrame(raw_response).replace('', np.nan)
        df_spark = spark.createDataFrame(df_pandas)

        return df_spark

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the API request: {e}")
    except ValueError as e:
        print(f"An error occurred while parsing the response JSON: {e}")

def extract_backfilling_spark(spark: SparkSession, link_api: str, start_date, current_date) -> DataFrame:
    df_milestones = None

    # Konversi ke string jika bukan string
    if not isinstance(start_date, str):
        start_date = start_date.strftime("%Y-%m-%d")
    if not isinstance(current_date, str):
        current_date = current_date.strftime("%Y-%m-%d")

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(current_date, "%Y-%m-%d")

    def append_and_dedup(base_df: DataFrame, new_df: DataFrame) -> DataFrame:
        if base_df is None:
            return new_df.dropDuplicates()
        return base_df.union(new_df).dropDuplicates()

    # Jika hanya 1 hari
    if start == end:
        list_parameter = {
            "start_date": start_date,
            "end_date": (end + timedelta(days=1)).strftime("%Y-%m-%d")
        }
        df_backfilling = extract_api_spark(spark, link_api, list_parameter)
        return df_backfilling.dropDuplicates()

    # Jika <= 1 tahun
    if end - start <= timedelta(days=365):
        list_parameter = {
            "start_date": start_date,
            "end_date": current_date
        }
        df_backfilling = extract_api_spark(spark, link_api, list_parameter)
        return df_backfilling.dropDuplicates()

    # Jika > 1 tahun
    while start + relativedelta(years=1) < end:
        temp_end = start + relativedelta(years=1)
        list_parameter = {
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": temp_end.strftime("%Y-%m-%d")
        }
        df_backfilling = extract_api_spark(spark, link_api, list_parameter)
        df_milestones = append_and_dedup(df_milestones, df_backfilling)
        start = temp_end

    # Sisa data terakhir
    list_parameter = {
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": current_date
    }
    df_backfilling = extract_api_spark(spark, link_api, list_parameter)
    df_milestones = append_and_dedup(df_milestones, df_backfilling)

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


