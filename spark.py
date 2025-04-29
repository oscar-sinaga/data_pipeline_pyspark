# import sys
# from pathlib import Path

# # Asumsikan kamu menjalankan notebook dari 'data_pipeline_pyspark/notebooks'
# # dan kamu ingin import dari 'data_pipeline_pyspark/src'
# project_root = Path.cwd().parent
# sys.path.append(str(project_root))
import pandas as pds

from src.utils.helper import startup_investments_engine_pyspark

from src.staging.extract.extract_db import extract_database
from src.staging.extract.extract_db_pyspark import extract_database as extract_database_pyspark

from src.staging.extract.extract_spreadsheet import extract_spreadsheet
from src.staging.extract.extract_api import extract_api_milestones
from src.staging.load.load import load_staging
from src.staging.extract.extract_spreadsheet import extract_spreadsheet, extract_sheet

from src.warehouse.extract.extract_db import extract_database as extract_staging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime

spark = SparkSession.builder \
    .appName("Pipeline Staging") \
    .master("local[1]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()



schema = StructType([
    StructField("step", StringType(), True),
    StructField("process", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source", StringType(), True),
    StructField("table_name", StringType(), True),
])

current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

data = [("staging", "extraction", "success", "database", 'acquisition')]
log_msg = spark.createDataFrame(data, schema=schema)
log_msg.show()

