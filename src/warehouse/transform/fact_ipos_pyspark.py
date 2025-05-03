from datetime import datetime
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import col, to_date, when, lit, coalesce, date_format
from pyspark.sql.types import StringType, IntegerType,LongType

from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target_pyspark
from src.utils.log import etl_log_pyspark

def transform_fact_ipos_spark(spark: SparkSession, data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    Transform IPO fact data using PySpark before loading to the data warehouse.
    """
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        process = "transformation"

        # Select relevant columns
        columns_to_picked = [
            'ipo_id', 
            'object_id',
            'public_at',
            'valuation_currency_code',
            'raised_currency_code',
            'valuation_amount',
            'raised_amount',
            'stock_symbol'
        ]
        data = data.select(*columns_to_picked)

        # Rename columns
        data = data \
            .withColumnRenamed('ipo_id', 'ipo_nk') \
            .withColumnRenamed('object_id', 'company_nk')

        # Deduplicate
        data = data.dropDuplicates(['ipo_nk'])

        # Cast nk columns
        data = data \
            .withColumn('ipo_nk', col('ipo_nk').cast(LongType()))
        
        # Handle nulls and format date
        data = data \
            .withColumn('public_at', to_date(col('public_at'))) \
            .withColumn('public_at', when(col('public_at').isNull(), lit("2100-01-01")).otherwise(col('public_at'))) \
            .withColumn('public_at', date_format(col('public_at'), 'yyyyMMdd').cast(IntegerType())) \
            # .withColumn('valuation_amount', coalesce(col('valuation_amount'), lit(0))) \
            # .withColumn('raised_amount', coalesce(col('raised_amount'), lit(0))) \
            # .withColumn('valuation_currency_code', coalesce(col('valuation_currency_code'), lit("N/A"))) \
            # .withColumn('raised_currency_code', coalesce(col('raised_currency_code'), lit("N/A"))) \
            # .withColumn('stock_symbol', coalesce(col('stock_symbol'), lit("N/A")))

        # Lookup dim_company to get company_id
        dim_company = extract_target_pyspark(spark, "dim_company").select("company_nk", "company_id")
        data = data.join(dim_company, on="company_nk", how="left")

        # Drop natural key
        data = data.drop("company_nk")

        # Log success
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
