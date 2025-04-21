import pandas as pd
import sqlalchemy
from src.utils.helper import log_engine, log_engine_pyspark, read_sql,MODEL_PATH_LOG_ETL
from pyspark.sql import SparkSession

def etl_log(log_msg: dict):
    try:
        # create connection to database
        conn = log_engine()
        
        # convert dictionary to dataframe
        df_log = pd.DataFrame([log_msg])

        #extract data log
        df_log.to_sql(name = "etl_log",  # Your log table
                        con = conn,
                        if_exists = "append",
                        index = False)
    except Exception as e:
        print("Can't save your log message. Cause: ", str(e))


def read_etl_log(filter_params: dict):
    """
    function read_etl_log that reads log information from the etl_log table and extracts the maximum etl_date for a specific process, step, table name, and status.
    """
    try:
        # create connection to database
        # conn = log_engine()
        
        # To help with the incremental process, get the etl_date from the relevant process
        """
        SELECT MAX(etl_date)
        FROM etl_log "
        WHERE 
            step = %s and
            table_name ilike %s and
            status = %s and
            process = %s
        """

        with log_engine().connect() as conn:

            # Load query
            query = sqlalchemy.text(read_sql(MODEL_PATH_LOG_ETL, "log"))

            # Execute the query
            df = pd.read_sql(sql=query, con=conn, params=filter_params)

            return df
        # query = sqlalchemy.text(read_sql(MODEL_PATH_LOG_ETL,"log"))

        # # Execute the query with pd.read_sql
        # df = pd.read_sql(sql=query, con=conn, params=(filter_params,))

        #return extracted data
        # return df
    except Exception as e:
        print("Can't execute your query. Cause: ", str(e))
