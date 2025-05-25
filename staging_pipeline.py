
from pyspark.sql import SparkSession
from src.staging.extract.extract_db_pyspark import extract_database as extract_database_pyspark
from src.staging.extract.extract_spreadsheet_pyspark import extract_spreadsheet as extract_spreadsheet_pyspark
from src.staging.extract.extract_api_pyspark import extract_api_milestones_spark,extract_api_milestones_spark
from src.staging.load.load_pyspark import load_staging_pyspark_upsert


def pipeline_staging():
    """Runs the staging pipeline for extracting and loading data from various sources."""
    print("==== Start Staging Pipeline ===")

    # Create a Spark session
    spark = SparkSession \
        .builder \
        .appName("Pipeline Staging") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    ### Extract Data
    ## Extract data from source database (startup_investments)
    # Acquisition data
    acquisition = extract_database_pyspark(spark, 'acquisition')

    # Company data
    company = extract_database_pyspark(spark, 'company')

    # Funding rounds data
    funding_rounds = extract_database_pyspark(spark, 'funding_rounds')

    # Funds data
    funds = extract_database_pyspark(spark, 'funds')

    # Investments data
    investments = extract_database_pyspark(spark, 'investments')

    # IPOs data
    ipos = extract_database_pyspark(spark, 'ipos')

    ## Extract data from spreadsheets
    # People data
    people_df = extract_spreadsheet_pyspark(spark, table_name='people')

    # Relationships data
    relationships_df = extract_spreadsheet_pyspark(spark, table_name='relationships')

    ## Extract data from APIs
    # Milestones data
    df_staging_api = extract_api_milestones_spark(spark, table_name='milestones')

    ### Load Data
    ## Load data into staging

    # Load acquisition data to staging
    load_staging_pyspark_upsert(spark, data=acquisition, schema='public', table_name='acquisition', idx_name='acquisition_id', source='database')

    # Load company data to staging
    load_staging_pyspark_upsert(spark, data=company, schema='public', table_name='company', idx_name='object_id', source='database')

    # Load funding rounds data to staging
    load_staging_pyspark_upsert(spark, data=funding_rounds, schema='public', table_name='funding_rounds', idx_name='funding_round_id', source='database')

    # Load funds data to staging
    load_staging_pyspark_upsert(spark, data=funds, schema='public', table_name='funds', idx_name='fund_id', source='database')

    # Load investments data to staging
    load_staging_pyspark_upsert(spark, data=investments, schema='public', table_name='investments', idx_name='investment_id', source='database')

    # Load IPOs data to staging
    load_staging_pyspark_upsert(spark, data=ipos, schema='public', table_name='ipos', idx_name='ipo_id', source='database')

    # Load people data to staging
    load_staging_pyspark_upsert(spark, data=people_df, schema='public', table_name='people', idx_name='person_id', source='spreadsheet')

    # Load relationships data to staging
    load_staging_pyspark_upsert(spark, data=relationships_df, schema='public', table_name='relationships', idx_name='relationship_id', source='spreadsheet')

    # Load milestones data to staging
    load_staging_pyspark_upsert(spark, data=df_staging_api, schema='public', table_name='milestones', idx_name='milestone_id', source='api')

    # Stop Spark Instance
    spark.stop()
    
    print("==== End Staging Pipeline ===")
