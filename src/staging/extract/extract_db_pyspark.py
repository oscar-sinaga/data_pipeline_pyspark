import pandas as pd
from datetime import datetime
from src.utils.helper import startup_investments_engine_pyspark
from src.utils.log import etl_log, read_etl_log,etl_log_pyspark
import numpy as np
from pyspark.sql import SparkSession

def extract_database(spark: SparkSession, table_name): 
    # get config
    DB_URL, DB_USER, DB_PASS = startup_investments_engine_pyspark()

    # set config
    connection_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver" # set driver postgres
    }
    

    current_timestamp = datetime.now()
    
    try:
        # read data
        df = spark.read.jdbc(url = DB_URL, 
                             table = table_name, 
                             properties = connection_properties)
            
        # log message
        log_msg = spark.sparkContext\
            .parallelize([("staging", "extraction", "success", "database", table_name, current_timestamp)])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date'])
        
        print(log_msg)
        
        return df
    except Exception as e:
        print(e)

        # log message
        log_msg = spark.sparkContext\
            .parallelize([("staging", "extraction", "failed", "database", table_name, current_timestamp, str(e))])\
            .toDF(['step', 'process', 'status', 'source', 'table_name', 'etl_date', 'error_msg'])
    finally:
        # load log
        etl_log_pyspark(spark, log_msg)

    