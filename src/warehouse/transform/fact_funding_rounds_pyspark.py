from datetime import datetime
from pyspark.sql.functions import to_date, col, when, lit, coalesce, date_format
from pyspark.sql.types import StringType, IntegerType, LongType
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target_pyspark
from src.utils.log import etl_log_pyspark

def transform_fact_funding_rounds_spark(spark: SparkSession, data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    Transform funding rounds fact data using PySpark before loading to the data warehouse.
    """
    # Current_timestamp
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        process = "transformation"

        # Select relevant columns
        columns_to_picked = [
            'funding_round_id', 
            'object_id',
            'funded_at',
            'funding_round_type',
            'funding_round_code',
            'raised_amount_usd',
            'pre_money_valuation_usd',
            'post_money_valuation_usd',
            'is_first_round',
            'is_last_round'
        ]
        data = data.select(*columns_to_picked)

        # Rename columns
        data = data \
            .withColumnRenamed('funding_round_id', 'funding_round_nk') \
            .withColumnRenamed('object_id', 'company_nk') \
            .withColumnRenamed('is_first_round', 'round_position_desc') \
            .withColumnRenamed('is_last_round', 'round_stage_desc')

        # Map boolean to string
        data = data \
            .withColumn('round_position_desc', when(col('round_position_desc') == True, 'First Round')
                        .otherwise('Not First Round')) \
            .withColumn('round_stage_desc', when(col('round_stage_desc') == True, 'Last Round')
                        .otherwise('Ongoing Round'))

        # Deduplication
        data = data.dropDuplicates(['funding_round_nk'])

        # Cast nk columns
        data = data \
            .withColumn('funding_round_nk', col('funding_round_nk').cast(LongType()))

        # Fill nulls and cast
        data = data \
            .withColumn('funded_at', to_date(col('funded_at'))) \
            .withColumn('funded_at', when(col('funded_at').isNull(), lit("2100-01-01")).otherwise(col('funded_at'))) \
            .withColumn('funded_at', date_format(col('funded_at'), 'yyyyMMdd').cast(IntegerType())) \
            # .withColumn('raised_amount_usd', coalesce(col('raised_amount_usd'), lit(0))) \
            # .withColumn('pre_money_valuation_usd', coalesce(col('pre_money_valuation_usd'), lit(0))) \
            # .withColumn('post_money_valuation_usd', coalesce(col('post_money_valuation_usd'), lit(0)))

        # Lookup company_id
        dim_company = extract_target_pyspark(spark, "dim_company").select("company_nk", "company_id")
        data = data.join(dim_company, on="company_nk", how="left")

        # Drop natural key
        data = data.drop("company_nk")
        
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

