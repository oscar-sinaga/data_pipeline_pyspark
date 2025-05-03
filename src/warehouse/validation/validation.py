import datetime
from minio import Minio
from io import BytesIO
import json
from src.utils.helper import ACCESS_KEY_MINIO,SECRET_KEY_MINIO,MINIO_PORT,MINIO_HOST
from pyspark.sql.functions import col
from pyspark.sql import DataFrame as SparkDataFrame

bucket_name = "validation-clinic"

def save_report(data, table_name):

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Initialize MinIO client
    client = Minio(f'{MINIO_HOST}:{MINIO_PORT}',
                    access_key=ACCESS_KEY_MINIO,
                    secret_key=SECRET_KEY_MINIO,
                    secure=False)

    # Make a bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Convert dict to JSON and then to bytes
    json_report = json.dumps(data)
    json_bytes = json_report.encode('utf-8')

    # Upload the CSV file to the bucket
    client.put_object(
        bucket_name=bucket_name,
        object_name=f"{table_name}_{current_date}.json", #name the fail source name and current etl date
        data=BytesIO(json_bytes),
        length=len(json_bytes),
        content_type='application/csv'
    )
    print(f"Save validation report as {table_name}_{current_date}.json")



def check_all_missing_except_id(df_spark: SparkDataFrame, report: dict, id_col: str, date_col: list = None):
    columns_to_check = [c for c in df_spark.columns if c != id_col]
    
    for colname in columns_to_check:
        if date_col and colname in date_col:
            df_missing = df_spark.filter(col(colname)==21000101)
        else:
            # Filter baris yang null di kolom ini
            df_missing = df_spark.filter(col(colname).isNull())
        
        # Hitung jumlah missing
        num_missing = df_missing.count()
        
        # Ambil daftar ID dari baris yang missing
        list_missing_ids = [row[id_col] for row in df_missing.select(id_col).collect()]
        
        # Masukkan ke report
        report["report"][colname] = {
            f"num_missing_{colname}": num_missing,
            f"list_{id_col}_missing_{colname}": list_missing_ids
        }
    
    return report


def report_validation(table_name: str, df_spark: SparkDataFrame, id_col: str, date_col: list = None):
    data = {
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d"),
        "table_name": table_name,
        "report":{}
    }
    data.update(check_all_missing_except_id(df_spark, data, id_col, date_col))

    save_report(data, table_name)