from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lit, concat_ws, when
from datetime import datetime

from src.warehouse.load.handle_error import handle_error
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lit, when
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from src.warehouse.load.handle_error import handle_error
from src.utils.log import etl_log_pyspark

def transform_dim_people_spark(spark:SparkSession, data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    Transform staging data before loading to the warehouse area (PySpark version).
    """
    current_timestamp  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        process = "transformation"

        # Select necessary columns
        columns_to_picked = [
            'people_id', 
            'first_name',
            'last_name',
            'affiliation_name',
            'birthplace'
        ]
        data = data.select(*columns_to_picked)

        # Derived column: full_name
        data = data.withColumn('full_name', concat_ws(' ', col('first_name'), col('last_name')))

        # Rename columns
        data = data.withColumnRenamed('people_id', 'people_nk') \
                   .withColumnRenamed('affiliation_name', 'affiliation')

        # Deduplicate based on people_nk
        data = data.dropDuplicates(['people_nk'])

        # Fill nulls
        data = data.withColumn('affiliation', when(col('affiliation').isNull(), lit('Unknown')).otherwise(col('affiliation')))
        data = data.withColumn('birthplace', when(col('birthplace').isNull(), lit('Unknown')).otherwise(col('birthplace')))

        # Cast people_nk to int
        data = data.withColumn('people_nk', col('people_nk').cast('int'))

        # Step 4: Buat log extraction sukses
        log_msg = spark.sparkContext.parallelize([(
        "warehouse", process, "success", "staging", table_name, current_timestamp
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date"])

        # Tambah kolom error_msg bernilai NULL
        log_msg = log_msg.withColumn("error_msg", lit(None).cast(StringType()))

        return data

    except Exception as e:
        print(e)
        # print("ETL transformation failed:", str(e))

        # Logging gagal
        log_msg = spark.sparkContext.parallelize([(
            "warehouse", process, "failed", "staging", table_name, current_timestamp, str(e)
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])
        # Handling error
        try:
            handle_error(data=data, table_name=table_name, process=process)
        except Exception as e:
            print(e)

    finally:
        log_msg.show()
        etl_log_pyspark(spark,log_msg)
