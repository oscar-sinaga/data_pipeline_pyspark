from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lit, when
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from src.warehouse.load.handle_error import handle_error
from src.utils.log import etl_log_pyspark

def transform_dim_company_spark(spark:SparkSession, data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    This function is used to transform the Spark DataFrame from the staging area before loading it into the warehouse area.
    """
    
    current_timestamp  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        process = "transformation"

        # Select columns
        columns_to_picked = [
            'object_id', 
            'description',
            'region',
            'city',
            'state_code',
            'country_code',
            'latitude',
            'longitude',
            # 'created_at',
            # 'updated_at'
        ]
        data = data.select(*columns_to_picked)

        # Rename column: object_id -> company_nk
        data = data.withColumnRenamed('object_id', 'company_nk')

        # Deduplicate based on company_nk
        data = data.dropDuplicates(['company_nk'])

        # Fill null values (set default values)
        data = data.withColumn('description', when(col('description').isNull(), lit('No Description')).otherwise(col('description')))
        data = data.withColumn('region', when(col('region').isNull(), lit('Unknown')).otherwise(col('region')))
        data = data.withColumn('city', when(col('city').isNull(), lit('Unknown')).otherwise(col('city')))
        data = data.withColumn('state_code', when(col('state_code').isNull(), lit('N/A')).otherwise(col('state_code')))
        data = data.withColumn('country_code', when(col('country_code').isNull(), lit('N/A')).otherwise(col('country_code')))
        data = data.withColumn('latitude', when(col('latitude').isNull(), lit(999)).otherwise(col('latitude')))
        data = data.withColumn('longitude', when(col('longitude').isNull(), lit(999)).otherwise(col('longitude')))

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

        # Handling error: save data to Object Storage
        try:
            handle_error(data=data, table_name=table_name, process=process)
        except Exception as err:
            print(f"Error saving failed data: {err}")
    
    finally:
        log_msg.show()
        etl_log_pyspark(spark, log_msg)
