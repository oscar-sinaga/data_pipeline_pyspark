from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
from src.utils.helper import wh_engine_pyspark
from src.utils.log import etl_log_pyspark
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

def load_warehouse_pyspark_upsert(spark: SparkSession, data, schema: str, table_name: str, idx_name: str, source: str, table_process:str):
    current_timestamp = datetime.now()

    try:
        # Setup koneksi PostgreSQL
        DB_URL, DB_USER, DB_PASS = wh_engine_pyspark()
        full_table = f"{schema}.{table_name}"
        connection_properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "org.postgresql.Driver"
        }

        # Step 1: Load data lama dari PostgreSQL
        try:
            existing_df = spark.read.jdbc(
                url=DB_URL,
                table=full_table,
                properties=connection_properties
            )
        except:
            existing_df = None  # Tabel mungkin belum ada

        # Step 2: Gabungkan data baru dengan data lama
        if existing_df:
            # Hapus data yang sudah ada berdasarkan idx_name
            non_matching_df = data.join(existing_df, on=idx_name, how="left_anti")  # Data yang tidak ada di existing_df
        else:
            non_matching_df = data  # Jika existing_df kosong, gunakan semua data baru

        # Step 3: Menambahkan data baru ke PostgreSQL
        non_matching_df.write.jdbc(
            url=DB_URL,
            table=full_table,
            mode="append",  # Append data baru ke tabel
            properties=connection_properties
        )

        # Step 4: Buat log load sukses
        log_msg = spark.sparkContext.parallelize([(
        "warehouse", "load", "success", source, table_process, current_timestamp
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date"])

        # Tambah kolom error_msg bernilai NULL
        log_msg = log_msg.withColumn("error_msg", lit(None).cast(StringType()))

    except Exception as e:
        # Logging gagal
        log_msg = spark.sparkContext.parallelize([(
            "warehouse", "load", "failed", source, table_process, current_timestamp, str(e)
        )]).toDF(["step", "process", "status", "source", "table_name", "etl_date", "error_msg"])

        print(f"Load failed: {e}")
    finally:
        log_msg.show()
        etl_log_pyspark(spark, log_msg)
