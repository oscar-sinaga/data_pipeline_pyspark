import pandas as pd
from datetime import datetime
from src.utils.helper import stg_engine_pyspark
from src.utils.log import etl_log_pyspark,read_etl_log_pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

def extract_database(spark: SparkSession, table_name: str):
    # Get DB connection config
    DB_URL, DB_USER, DB_PASS = stg_engine_pyspark()
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver"
    }

    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Step 1: Ambil tanggal terakhir dari proses sebelumnya (load -> success)
        filter_log = {
            "step_name": "warehouse",
            "table_name": table_name,
            "status": "success",
            "process": "load"
        }

        etl_date_df = read_etl_log_pyspark(spark, filter_log)

        if etl_date_df is None or etl_date_df.count() == 0 or etl_date_df.first()[0] is None:
            etl_date = '1111-01-01 00:00:00'
        else:
            etl_date = etl_date_df.first()[0].strftime('%Y-%m-%d %H:%M:%S')

        # Step 2: Bangun query SQL untuk incremental extract
        sql_query = f"(SELECT * FROM {table_name} WHERE created_at > '{etl_date}'::timestamp) AS subquery"

        # Step 3: Load data dari database PostgreSQL
        df = spark.read.jdbc(
            url=DB_URL,
            table=sql_query,
            properties=connection_properties
        )

        df = df.na.replace("", None)

        # Step 4: Buat log extraction sukses
        log_msg = spark.sparkContext.parallelize([(
        "warehouse", "extraction", "success", "database", table_name, current_timestamp
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date"])

        # Tambah kolom error_msg bernilai NULL
        log_msg = log_msg.withColumn("error_msg", lit(None).cast(StringType()))

        return df

    except Exception as e:
        print("ETL extraction failed:", str(e))

        # Logging gagal
        log_msg = spark.sparkContext.parallelize([(
            "warehouse", "extraction", "failed", "database", table_name, current_timestamp, str(e)
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

        return None
    finally:
        log_msg.show()
        etl_log_pyspark(spark, log_msg)