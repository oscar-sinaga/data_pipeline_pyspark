from datetime import datetime
from pyspark.sql.functions import to_date, col, when, lit, coalesce, date_format
from pyspark.sql.types import StringType, IntegerType
from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target_pyspark
from src.utils.log import etl_log_pyspark
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

def transform_fact_acquisitions_spark(spark: SparkSession, data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    This function transforms acquisition fact data using PySpark before loading to the data warehouse.
    """
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        process = "transformation"

        # Pick only selected columns
        columns_to_picked = [
            'acquisition_id', 
            'acquiring_object_id',
            'acquired_object_id',
            'acquired_at',
            'price_amount',
            'price_currency_code',
            'term_code'
        ]
        data = data.select(*columns_to_picked)

        # Rename columns
        data = data \
            .withColumnRenamed('acquisition_id', 'acquisition_nk') \
            .withColumnRenamed('acquiring_object_id', 'acquiring_company_nk') \
            .withColumnRenamed('acquired_object_id', 'acquired_company_nk')

        # Deduplicate
        data = data.dropDuplicates(['acquisition_nk'])

        # Fill NA and cast types
        data = data \
                    .withColumn('acquired_at', to_date(col('acquired_at'))) \
                    .withColumn('acquired_at', when(col('acquired_at').isNull(), lit("2100-01-01")).otherwise(col('acquired_at'))) \
                    .withColumn('acquired_at', date_format(col('acquired_at'), 'yyyyMMdd').cast(IntegerType())) \
                    .withColumn('term_code', coalesce(col('term_code'), lit('Unknown'))) \
                    .withColumn('price_currency_code', coalesce(col('price_currency_code'), lit('N/A'))) \
                    .withColumn('price_amount', coalesce(col('price_amount'), lit(0)))
                    
        # Load dimension company for ID lookups
        dim_company = extract_target_pyspark(spark, "dim_company")
        dim_company = dim_company.select("company_nk", "company_id")

        # Join to get acquiring_company_id
        data = data.join(
            dim_company.withColumnRenamed("company_nk", "acquiring_company_nk"),
            on="acquiring_company_nk", how="left"
        ).withColumnRenamed("company_id", "acquiring_company_id")

        # Join to get acquired_company_id
        data = data.join(
            dim_company.withColumnRenamed("company_nk", "acquired_company_nk"),
            on="acquired_company_nk", how="left"
        ).withColumnRenamed("company_id", "acquired_company_id")

        # Drop natural key columns
        data = data.drop("acquiring_company_nk", "acquired_company_nk")

         # Logging success
        log_msg = spark.sparkContext.parallelize([(
            "warehouse", process, "success", "staging", table_name, current_timestamp
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date"]) \
         .withColumn("error_msg", lit(None).cast(StringType()))

        return data

    except Exception as e:
        print(e)
        log_msg = spark.sparkContext.parallelize([(
            "warehouse", process, "failed", "staging", table_name, current_timestamp, str(e)
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

        try:
            handle_error(data=data, table_name=table_name, process=process)
        except Exception as err:
            print(err)
    finally:
        log_msg.show()
        etl_log_pyspark(spark, log_msg)

