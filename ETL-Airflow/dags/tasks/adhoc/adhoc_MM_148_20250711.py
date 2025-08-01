from airflow.decorators import task
import logging
from tasks.utils import (
    read_data,
    abort_session,
    get_spark_session
)


@task(task_id="m_load_legacy_tables_to_gcs")
def push_data_to_reporting():
    try:
        spark = get_spark_session()
        table_name = 'customer_sales_report'
        df = read_data(spark, f"legacy.{table_name}")
        logging.info(f"Loading {df.count()} records into {table_name}")
        df.write.mode("overwrite").parquet(
            f"gs://reporting-lgcy/{table_name}"
        )
        logging.info(
            f"Loaded into Parquet File : gs://reporting-lgcy/{table_name}"
        )
    except Exception as e:
        logging.error(e)
        raise
    finally:
        abort_session(spark)
    return f"{table_name} pushed to reporting"
