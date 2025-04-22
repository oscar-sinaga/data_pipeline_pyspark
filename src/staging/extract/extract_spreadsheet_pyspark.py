from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from dotenv import load_dotenv
import os
import csv
from datetime import datetime
import numpy as np 
from pathlib import Path
from src.utils.helper import CRED_PATH,KEY_SPREADSHEET_PEOPLE,KEY_SPREADSHEET_RELATIONSHIPS,stg_engine
from src.utils.log import etl_log_pyspark,read_etl_log_pyspark
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

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


def extract_sheet_spark(spark: SparkSession, table_name: str):
    # Ambil worksheet
    sheet_result = init_key_file(table_name)
    worksheet_result = sheet_result.get_worksheet(0)

    # Ambil semua data
    all_data = worksheet_result.get_all_values()
    header = all_data[0]
    rows = all_data[1:]

    # Ganti '' jadi None (biar jadi null di Spark)
    cleaned_rows = [[None if val == '' else val for val in row] for row in rows]

    # Buat schema dari header
    schema = StructType([StructField(col_name, StringType(), True) for col_name in header])

    # Buat Spark DataFrame
    spark_df = spark.createDataFrame(cleaned_rows, schema=schema)

    return spark_df


def extract_spreadsheet(spark: SparkSession,table_name: str):
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # extract data
        if table_name=='people':
            df_data = extract_sheet_spark(spark,table_name = table_name)
            # Tambahkan kolom created_at dengan timestamp sekarang
            df_data = df_data.withColumn("created_at", lit(current_timestamp))

        elif table_name=='relationships':
            # Get date from previous process
            filter_log = {"step_name": "staging",
                        "table_name": table_name,
                        "status": "success",
                        "process": "load"}
            etl_date_df = read_etl_log_pyspark(spark, filter_log)

            # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
            # Otherwise, retrieve data added since the last successful extraction (etl_date).
            etl_date_df = read_etl_log_pyspark(spark, filter_log)

            if etl_date_df is None or etl_date_df.count() == 0 or etl_date_df.first()[0] is None:
                etl_date = '1111-01-01 00:00:00'
            else:
                etl_date = etl_date_df.first()[0].strftime('%Y-%m-%d %H:%M:%S')
            
                # Ubah string ke timestamp Spark (tanpa Pandas)
                etl_date = lit(etl_date).cast("timestamp")

                # Load dan konversi kolom created_at ke timestamp
                df_data = extract_sheet_spark(spark, table_name=table_name)
                df_data = df_data.withColumn("created_at", col("created_at").cast("timestamp"))

                # Filter hanya data baru
                df_data = df_data.filter(col("created_at") > etl_date)
        
        # Step 4: Buat log extraction sukses
        log_msg = spark.sparkContext.parallelize([(
        "staging", "extraction", "success", "spreadsheet", table_name, current_timestamp
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date"])

        # Tambah kolom error_msg bernilai NULL
        log_msg = log_msg.withColumn("error_msg", lit(None).cast(StringType()))

        return df_data
    except Exception as e:
        # Logging gagal
        log_msg = spark.sparkContext.parallelize([(
            "staging", "extraction", "failed", "spreadsheet", table_name, current_timestamp, str(e)
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

    finally:
        log_msg.show()
        etl_log_pyspark(spark, log_msg)