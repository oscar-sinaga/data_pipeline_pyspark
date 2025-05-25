from staging_pipeline import pipeline_staging
from warehouse_pipeline import pipeline_warehouse


if __name__ == "__main__":
    pipeline_staging()
    pipeline_warehouse()