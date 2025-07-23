from airflow.decorators import task
import logging
from tasks.utils import (
    get_spark_session,
    read_data,
    write_to_gcs,
    abort_session,
    fetch_env_schema
)
from pyspark.sql.functions import col, current_date
from datetime import timedelta


@task(
        task_id="m_load_legacy_tables_to_gcs",
        retries=3,
        retry_delay=timedelta(minutes=1)
)
def push_data_to_reporting(env, table_name: str):
    try:
        schema_dict = fetch_env_schema(env)
        legacy = schema_dict['legacy']
        gcs_legacy = schema_dict['gcs_legacy']
        spark = get_spark_session()
        df = read_data(spark, f"{legacy}.{table_name}")
        today_df = df.filter(col("DAY_DT") == current_date())
        logging.info(f"Loading {today_df.count()} records into {table_name}")
        write_to_gcs(today_df, gcs_legacy, table_name)
    except Exception as e:
        logging.error(e)
    finally:
        abort_session(spark)
    return f"{table_name} done"
