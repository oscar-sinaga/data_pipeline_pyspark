from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lit, concat_ws, when
from datetime import datetime

from src.warehouse.load.handle_error import handle_error
from src.utils.log import etl_log

def transform_dim_people_spark(data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    Transform staging data before loading to the warehouse area (PySpark version).
    """
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

        log_msg = {
            "step": "warehouse",
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
            "step": "warehouse",
            "process": process,
            "status": "failed",
            "source": "staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_msg": str(e)
        }

        # Handling error
        try:
            handle_error(data=data, table_name=table_name, process=process)
        except Exception as e:
            print(e)

    finally:
        etl_log(log_msg)
