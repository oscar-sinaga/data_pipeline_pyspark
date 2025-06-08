from datetime import datetime
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lit, when, to_date, date_format
from pyspark.sql.types import IntegerType, StringType, LongType

from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target_pyspark
from src.utils.log import etl_log_pyspark

def transform_investments_spark(spark: SparkSession, data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    Transform Investments data using PySpark before before combining with table funding rounds to the data warehouse.
    """
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        process = "transformation"

        # Select relevant columns
        columns_to_picked = [
            'investment_id', 
            'funding_round_id',
            'investor_object_id'
        ]
        data = data.select(*columns_to_picked)

        # Rename columns
        data = data \
            .withColumnRenamed('investment_id', 'investment_nk') \
            .withColumnRenamed('funding_round_id', 'funding_round_nk') \
            .withColumnRenamed('investor_object_id', 'investor_company_nk')

        # Deduplication
        data = data.dropDuplicates(['investment_nk'])

        # Convert data types
        data = data \
            .withColumn('investment_nk', col('investment_nk').cast(LongType())) \
            .withColumn('funding_round_nk', col('funding_round_nk').cast(IntegerType()))

        # Lookup `company_id` from `dim_company` for investor
        dim_company = extract_target_pyspark(spark, 'dim_company').select('company_nk', 'company_id')

        data = data \
            .join(dim_company.withColumnRenamed("company_nk", "investor_company_nk").withColumnRenamed("company_id", "investor_company_id"),
                  on="investor_company_nk", how="left")

        # Drop natural keys
        data = data.drop('investor_company_nk')

        # Success log message
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


def transform_funding_rounds_spark(spark: SparkSession, data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    Transform funding rounds in staging data using PySpark before combining with table investments to the data warehouse.
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
            'raised_amount',
            'pre_money_valuation',
            'post_money_valuation',
            'participants',
            'is_first_round',
            'is_last_round'
        ]
        data = data.select(*columns_to_picked)

        # Rename columns
        data = data \
            .withColumnRenamed('object_id', 'investee_company_nk') \
            .withColumnRenamed('funding_round_id', 'funding_round_nk') \
            .withColumnRenamed('participants', 'number_of_participants') \
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
            .withColumn('funding_round_nk', col('funding_round_nk').cast(LongType()))\
            .withColumn('number_of_participants', col('number_of_participants').cast(IntegerType()))


        # Fill nulls and cast
        data = data \
            .withColumn('funded_at', to_date(col('funded_at'))) \
            .withColumn('funded_at', when(col('funded_at').isNull(), lit("2100-01-01")).otherwise(col('funded_at'))) \
            .withColumn('funded_at', date_format(col('funded_at'), 'yyyyMMdd').cast(IntegerType())) \
            # .withColumn('raised_amount', coalesce(col('raised_amount'), lit(0))) \
            # .withColumn('pre_money_valuation', coalesce(col('pre_money_valuation'), lit(0))) \
            # .withColumn('post_money_valuation', coalesce(col('post_money_valuation'), lit(0)))

        # Lookup company_id
        dim_company = extract_target_pyspark(spark, "dim_company").select("company_nk", "company_id")
        data = data \
            .join(dim_company.withColumnRenamed("company_nk", "investee_company_nk").withColumnRenamed("company_id", "investee_company_id"),
                  on="investee_company_nk", how="left")
        # Drop natural key
        data = data.drop("investee_company_nk")

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


def transform_fact_investment_round_participation(spark: SparkSession, 
                                                  data_investments_transformed: SparkDataFrame,
                                                  data_funding_rounds_transformed: SparkDataFrame,
                                                  table_name: str) -> SparkDataFrame:
    """
    Combine tranformed Investments and transformed funding rounds data using PySpark to the data warehouse.
    """
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        process = "Combine Transformations"
        table_names= 'investments, funding_rounds'

        data = data_investments_transformed.join(data_funding_rounds_transformed, on='funding_round_nk', how='left')

        # Success log message
        log_msg = spark.sparkContext.parallelize([(
            "warehouse", process, "success", "staging", table_name, current_timestamp
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date"]) \
        .withColumn("error_msg", lit(None).cast(StringType()))

        return data

    except Exception as e:
        print(e)
        log_msg = spark.sparkContext.parallelize([(
            "warehouse", process, "failed", "staging", table_names, current_timestamp, str(e)
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

        try:
            handle_error(data=data, table_name=table_name, process=process)
        except Exception as err:
            print(err)
    finally:
        log_msg.show()
        etl_log_pyspark(spark, log_msg)
