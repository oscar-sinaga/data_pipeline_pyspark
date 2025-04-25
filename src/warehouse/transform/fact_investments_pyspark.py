from datetime import datetime
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StringType

from src.warehouse.load.handle_error import handle_error
from src.utils.helper import extract_target_pyspark
from src.utils.log import etl_log_pyspark

def transform_fact_investments_spark(spark: SparkSession, data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    Transform Investments fact data using PySpark before loading to the data warehouse.
    """
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        process = "transformation"

        # Select relevant columns
        columns_to_picked = [
            'investment_id', 
            'funding_round_id',
            'funded_object_id',
            'investor_object_id'
        ]
        data = data.select(*columns_to_picked)

        # Rename columns
        data = data \
            .withColumnRenamed('investment_id', 'investment_nk') \
            .withColumnRenamed('funding_round_id', 'funding_round_nk') \
            .withColumnRenamed('funded_object_id', 'investee_company_nk') \
            .withColumnRenamed('investor_object_id', 'investor_company_nk')

        # Deduplication
        data = data.dropDuplicates(['investment_nk'])

        # Convert data types
        data = data \
            .withColumn('investment_nk', col('investment_nk').cast(IntegerType())) \
            .withColumn('funding_round_nk', col('funding_round_nk').cast(IntegerType()))

        # Lookup `funding_round_id` from `fact_funding_rounds`
        fact_funding_rounds = extract_target_pyspark(spark, 'fact_funding_rounds').select('funding_round_nk', 'funding_round_id')
        data = data.join(fact_funding_rounds, on='funding_round_nk', how='left')

        # Lookup `company_id` from `dim_company` for both investee and investor
        dim_company = extract_target_pyspark(spark, 'dim_company').select('company_nk', 'company_id')

        data = data \
            .join(dim_company.withColumnRenamed("company_nk", "investee_company_nk").withColumnRenamed("company_id", "investee_company_id"),
                  on="investee_company_nk", how="left") \
            .join(dim_company.withColumnRenamed("company_nk", "investor_company_nk").withColumnRenamed("company_id", "investor_company_id"),
                  on="investor_company_nk", how="left")

        # Drop natural keys
        data = data.drop('funding_round_nk', 'investee_company_nk', 'investor_company_nk')

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
