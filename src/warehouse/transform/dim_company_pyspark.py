from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lit, when
from datetime import datetime

from src.warehouse.load.handle_error import handle_error
from src.utils.log import etl_log

def transform_dim_company_spark(data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    This function is used to transform the Spark DataFrame from the staging area before loading it into the warehouse area.
    """
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

        log_msg = {
            "step" : "warehouse",
            "process": process,
            "status": "success",
            "source": "staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        return data
    
    except Exception as e:
        print(e)
        log_msg = {
            "step" : "warehouse",
            "process": process,
            "status": "failed",
            "source": "staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }

        # Handling error: save data to Object Storage
        try:
            handle_error(data=data, table_name=table_name, process=process)
        except Exception as err:
            print(f"Error saving failed data: {err}")
    
    finally:
        # Save the log message
        etl_log(log_msg)
