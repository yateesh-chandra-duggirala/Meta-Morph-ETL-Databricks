from airflow.decorators import task
import logging
from tasks.utils import (
    write_into_table,
    read_data,
    write_to_gcs
)
from pyspark.sql.functions import col, current_date


# push_table_to_gcs_file
@task(task_id="m_load_prod_supplier_performance")
def load_prod_supplier_performance(spark):
    """
    Consumes the table and writes into file.
    """
    try:
        supplier_df = read_data(spark, "dev_legacy.supplier_performance")
        logging.info(supplier_df.count())
        write_into_table(
            table="supplier_performance",
            data_frame=supplier_df,
            schema='legacy',
            strategy='append'
        )
    except Exception as e:
        logging.error(e)
        raise e
    return "Loaded data from dev to Prod..!"


@task(task_id="m_load_legacy_tables_to_gcs")
def push_data_to_reporting(spark):
    try:
        table_name = 'supplier_performance'
        df = read_data(spark, f"legacy.{table_name}")
        today_df = df.filter(col("DAY_DT") == current_date())
        logging.info(f"Loading {today_df.count()} records into {table_name}")
        write_to_gcs(today_df, table_name)
    except Exception as e:
        logging.error(e)
        raise
    return f"{table_name} pushed to reporting"
