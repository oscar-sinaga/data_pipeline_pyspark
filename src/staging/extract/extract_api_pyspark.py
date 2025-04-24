import pandas as pd
from dotenv import load_dotenv
import requests
from datetime import datetime
import numpy as np
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from src.utils.log import etl_log_pyspark, read_etl_log_pyspark
from datetime import timedelta
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit,when
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

LINK_API_MILESTONE = "https://api-milestones.vercel.app/api/data"

link_api = LINK_API_MILESTONE


def extract_api_spark(spark: SparkSession, link_api: str, list_parameter: dict):
    try:
        # Call the API
        resp = requests.get(link_api, params=list_parameter)
        raw_response = resp.json()

        # Jika data kosong, kembalikan DataFrame kosong dengan schema dummy
        if not raw_response:
            # Misal: buat schema kosong tapi tetap sesuai dengan struktur yang diharapkan
            empty_schema = StructType([
                StructField("created_at", StringType(), True),
                StructField("description", StringType(), True),
                StructField("milestone_at", StringType(), True),
                StructField("milestone_code", StringType(), True),
                StructField("milestone_id", StringType(), True),
                StructField("object_id", StringType(), True),
                StructField("source_description", StringType(), True),
                StructField("source_url", StringType(), True),
                StructField("updated_at", StringType(), True),

            ])
            return spark.createDataFrame([], schema=empty_schema)

        # Convert ke Pandas DataFrame
        df = pd.DataFrame(raw_response)

        # Replace empty or NaN values with Python None
        df = df.replace([np.nan, ''], None)

        # Convert all columns to string (Spark expects uniform types)
        df = df.astype(str)

        # Create Spark DataFrame
        df_spark = spark.createDataFrame(df)

        # Replace string 'nan' or 'None' (if any got through) with null
        for col_name in df_spark.columns:
            df_spark = df_spark.withColumn(
                col_name,
                when((col(col_name) == 'nan') | (col(col_name) == 'None'), None).otherwise(col(col_name))
            )

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
        if new_df is None or new_df.rdd.isEmpty():
            return base_df
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
def extract_api_milestones_spark(spark: SparkSession, table_name: str):
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Ambil tanggal terakhir sukses load dari log
        filter_log = {
            "step_name": "staging",
            "table_name": table_name,
            "status": "success",
            "process": "load"
        }

        etl_date_df = read_etl_log_pyspark(spark, filter_log)

        if etl_date_df is None or etl_date_df.count() == 0 or etl_date_df.first()[0] is None:
            etl_date = "1950-01-01"
        else:
            etl_date = etl_date_df.first()[0].strftime("%Y-%m-%d")

        # Backfilling berdasarkan rentang tahun
        df = extract_backfilling_spark(spark, link_api, etl_date, current_date)

        # Cast created_at dan filter hanya yang lebih baru dari etl_date
        df = df.withColumn("created_at", col("created_at").cast("timestamp"))
        df = df.filter(col("created_at") > etl_date)

         # Step 4: Buat log extraction sukses
        log_msg = spark.sparkContext.parallelize([(
        "staging", "extraction", "success", "api", table_name, current_timestamp
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date"])

        # Tambah kolom error_msg bernilai NULL
        log_msg = log_msg.withColumn("error_msg", lit(None).cast(StringType()))

        return df
    
    except Exception as e:
        print("ETL extraction failed:", str(e))

        # Logging gagal
        log_msg = spark.sparkContext.parallelize([(
            "staging", "extraction", "failed", "api", table_name, current_timestamp, str(e)
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

        return None
    finally:
        log_msg.show()
        etl_log_pyspark(spark, log_msg)