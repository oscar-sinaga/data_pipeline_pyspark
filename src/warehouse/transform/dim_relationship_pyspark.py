from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_date, date_format
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime
from pyspark.sql.functions import to_timestamp
from src.warehouse.load.handle_error import handle_error
from src.utils.log import etl_log_pyspark
from src.utils.helper import extract_target_pyspark  # diasumsikan kamu punya ini versi Spark

def transform_dim_relationship_spark(spark: SparkSession, data: SparkDataFrame, table_name: str) -> SparkDataFrame:
    """
    Transform relationship data from staging to warehouse-ready Spark DataFrame.
    """
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        process = "transformation"

        # Select relevant columns
        columns_to_picked = [
            'relationship_id', 
            'person_object_id',
            'relationship_object_id',
            'title',
            'start_at',
            'end_at',
            'is_past',
            'sequence',
        ]
        data = data.select(*columns_to_picked)

        # Rename columns
        data = data \
            .withColumnRenamed("relationship_id", "relationship_nk") \
            .withColumnRenamed("person_object_id", "people_nk") \
            .withColumnRenamed("relationship_object_id", "company_nk") \
            .withColumnRenamed("is_past", "relationship_status") \
            .withColumnRenamed("sequence", "relationship_order")

        # Deduplicate
        data = data.dropDuplicates(["relationship_nk"])

        # Fill NULLs
        data = data.withColumn("title", when(col("title").isNull(), lit("Unknown")).otherwise(col("title")))
        data = data.withColumn("relationship_status", when(col("relationship_status").isNull(), lit("Unknown")).otherwise(col("relationship_status")))
        data = data.withColumn("relationship_order", when(col("relationship_order").isNull(), lit(0)).otherwise(col("relationship_order")))

        # Convert start_at and end_at to int date format (yyyyMMdd), null jadi 21000101
        for col_name in ['start_at', 'end_at']:
            data = data.withColumn(
                col_name,
                date_format(
                    to_timestamp(col(col_name), 'yyyy-MM-dd HH:mm:ss.SSS'),
                    'yyyyMMdd'
                ).cast(IntegerType())
            )
            data = data.withColumn(col_name, when(col(col_name).isNull(), lit(21000101)).otherwise(col(col_name)))

        # Convert NK to ID by joining with dim_people
        dim_people = extract_target_pyspark(spark, "dim_people")  # dim_people has people_nk, people_id
        data = data.join(dim_people.select("people_nk", "people_id"), on="people_nk", how="left")

        # Join with dim_company
        dim_company = extract_target_pyspark(spark, "dim_company")  # dim_company has company_nk, company_id
        data = data.join(dim_company.select("company_nk", "company_id"), on="company_nk", how="left")

        # Drop NK columns
        data = data.drop("people_nk", "company_nk")

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
            print(f"Error saving failed data: {err}")
    
    finally:
        log_msg.show()
        etl_log_pyspark(spark, log_msg)
