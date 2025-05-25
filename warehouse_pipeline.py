from src.warehouse.extract.extract_db import extract_database as extract_staging
from pyspark.sql import SparkSession
from src.warehouse.extract.extract_db_pyspark import extract_database as extract_db_pyspark_staging
from src.warehouse.extract.extract_db_pyspark import extract_database as extract_db_pyspark_staging
from src.warehouse.transform.dim_company_pyspark import transform_dim_company_spark
from src.warehouse.transform.dim_people_pyspark import transform_dim_people_spark
from src.warehouse.transform.dim_relationship_pyspark import transform_dim_relationship_spark
from src.warehouse.transform.fact_acquisitions_pyspark import transform_fact_acquisitions_spark
from src.warehouse.transform.fact_funding_rounds_pyspark import transform_fact_funding_rounds_spark
from src.warehouse.transform.fact_funds_pyspark import transform_fact_funds_spark
from src.warehouse.transform.fact_ipos_pyspark import transform_fact_ipos_spark
from src.warehouse.transform.fact_milestones_pyspark import transform_fact_milestones_spark
from src.warehouse.transform.fact_investments_pyspark import transform_fact_investments_spark
from src.warehouse.load.load_pyspark import load_warehouse_pyspark_upsert
from src.warehouse.validation.validation import report_validation

def pipeline_warehouse():
    print("==== Start Warehouse Pipeline ===")

    # Create a Spark session
    spark = SparkSession \
        .builder \
        .appName("Pipeline Warehouse") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Extract data from staging
    print("Extracting data from staging...")
    people_staging = extract_db_pyspark_staging(spark,table_name='people')
    company_staging = extract_db_pyspark_staging(spark,table_name='company')
    relationships_staging = extract_db_pyspark_staging(spark,table_name='relationships')
    investments_staging = extract_db_pyspark_staging(spark,table_name='investments')
    ipos_staging = extract_db_pyspark_staging(spark,table_name='ipos')
    acquisition_staging = extract_db_pyspark_staging(spark,table_name='acquisition')
    funding_rounds_staging = extract_db_pyspark_staging(spark,table_name='funding_rounds')
    funds_staging = extract_db_pyspark_staging(spark,table_name='funds')
    milestone_staging = extract_db_pyspark_staging(spark,table_name='milestones')


    #### Transform, Validate and Load Data
    print("Transforming, Validating and Loading data...")
    ### Table Dimension
    print("Transforming, Validating and Loading data for dimension tables...")

    ## People
    # transform
    dim_people = transform_dim_people_spark(spark,people_staging,'people')
    # validate
    report_validation(table_name='people', df_spark=dim_people, id_col='people_nk')
    # load
    load_warehouse_pyspark_upsert(spark=spark,data=dim_people, table_name='dim_people', schema='public', 
                idx_name='people_nk', source='staging',table_process='people')

    ## Company
    # transform
    dim_company = transform_dim_company_spark(spark,company_staging,'company')
    # validate
    report_validation(table_name='company', df_spark=dim_company, id_col='company_nk')
    # load
    load_warehouse_pyspark_upsert(spark=spark,data=dim_company, table_name='dim_company', schema='public', 
                idx_name='company_nk', source='staging',table_process='company')

    ## relationship
    # transform
    dim_relationship = transform_dim_relationship_spark(spark,relationships_staging,'relationships')
    # validate
    report_validation(table_name='relationships', df_spark=dim_relationship, id_col='relationship_nk')
    # load
    load_warehouse_pyspark_upsert(spark=spark,data=dim_relationship, table_name='dim_relationship', schema='public', 
                idx_name='relationship_nk', source='staging',table_process='relationships')
    
    ### Table Fact
    print("Transforming, Validating and Loading data for fact tables...")
    
    ## Investments

    # transform
    fact_investments = transform_fact_investments_spark(spark,investments_staging,'investments')
    # validate
    report_validation(table_name='investments', df_spark=fact_investments, id_col='investment_nk')
    # load
    load_warehouse_pyspark_upsert(spark=spark,data=fact_investments, table_name='fact_investments', schema='public', 
                idx_name='investment_nk', source='staging',table_process='investments')
    
    ## IPOs
    # transform
    fact_ipos = transform_fact_ipos_spark(spark,ipos_staging,'ipos')
    # validate
    report_validation(table_name='ipos', df_spark=fact_ipos, id_col='ipo_nk')
    # load
    load_warehouse_pyspark_upsert(spark=spark,data=fact_ipos, table_name='fact_ipos', schema='public', 
                idx_name='ipo_nk', source='staging',table_process='ipos')

    ## Acquisition
    # transform
    fact_acquisition = transform_fact_acquisitions_spark(spark,acquisition_staging,'acquisition')
    # validate
    report_validation(table_name='acquisition', df_spark=fact_acquisition, id_col='acquisition_nk')
    # load
    load_warehouse_pyspark_upsert(spark=spark,data=fact_acquisition, table_name='fact_acquisition', schema='public', 
                idx_name='acquisition_nk', source='staging',table_process='acquisition')

    ## Funding Rounds
    # transform
    fact_funding_rounds = transform_fact_funding_rounds_spark(spark,funding_rounds_staging,'funding_rounds')
    # validate
    report_validation(table_name='funding_rounds', df_spark=fact_funding_rounds, id_col='funding_round_nk')
    # load
    load_warehouse_pyspark_upsert(spark=spark,data=fact_funding_rounds, table_name='fact_funding_rounds', schema='public', 
                idx_name='funding_round_nk', source='staging',table_process='funding_rounds')
    
    ## Milestones
    # transform
    fact_milestones = transform_fact_milestones_spark(spark,milestone_staging,'milestones')
    # validate
    report_validation(table_name='milestones', df_spark=fact_milestones, id_col='milestone_nk')
    # load
    load_warehouse_pyspark_upsert(spark=spark,data=fact_milestones, table_name='fact_milestones', schema='public', 
                idx_name='milestone_nk', source='staging',table_process='milestones')
    
    ## Funds
    # transform
    fact_funds = transform_fact_funds_spark(spark,funds_staging,'funds')
    # validate
    report_validation(table_name='funds', df_spark=fact_funds, id_col='fund_nk')
    # load
    load_warehouse_pyspark_upsert(spark=spark,data=fact_funds, table_name='fact_funds', schema='public', 
                idx_name='fund_nk', source='staging',table_process='funds')
    


    # Stop Spark Instance
    spark.stop()
    
    print("==== End Warehouse Pipeline ===")