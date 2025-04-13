import re
import pandas as pd
import datetime
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv
import os
import json
from src.utils.helper import ACCESS_KEY_MINIO,SECRET_KEY_MINIO,ERROR_STAGING_SI_BUCKET_NAME,MINIO_PORT,MINIO_HOST

bucket_name = ERROR_STAGING_SI_BUCKET_NAME

def handle_error(data, table_name:str, process:str):

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Initialize MinIO client
    client = Minio(f'{MINIO_HOST}:{MINIO_PORT}',
                    access_key=ACCESS_KEY_MINIO,
                    secret_key=SECRET_KEY_MINIO,
                    secure=False)

    # Make a bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Convert DataFrame to CSV and then to bytes
    csv_bytes = data.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    # Upload the CSV file to the bucket
    client.put_object(
        bucket_name=bucket_name,
        object_name=f"{process}_{table_name}_{current_date}.csv", #name the fail source name and current etl date
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='application/csv'
    )

    # List objects in the bucket
    objects = client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        print(obj.object_name)