# adhoc_pipeline_metamorph.py
# Import Libraries
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pytz
import os
from tasks.adhoc.adhoc_reload_tables_task import *

# Date/count generator
def freq(today):
    result = []
    for idx, i in enumerate(range(today, today + 5)):
        result.append((str(i), 4 - idx))
    return result

@dag(
    dag_id='adhoc_reload_metamorph',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=7,
)
def ingestion():

    india_tz = pytz.timezone("Asia/Kolkata")
    today = datetime.now(india_tz).strftime("%Y%m%d")
    pairs = freq(int(today)-4)

    prev_group = None

    for date, cnt in pairs:
        with TaskGroup(group_id=f"ingest_data_for_{date}") as group:

            env = os.getenv('ENV', 'dev')
            # Individual task calls
            suppliers = supplier_data_ingestion(env, date, cnt)
            customers = customer_data_ingestion(env, date, cnt)
            products = products_data_ingestion(env, date, cnt)
            sales = sales_data_ingestion(env, date, cnt)

            supplier_perf = suppliers_performance_ingestion(env, date, cnt)
            product_perf = product_performance_ingestion(env, date, cnt)
            customer_sales = customer_sales_report_ingestion(env, date, cnt)

            # Set dependencies inside each group
            for upstream in [suppliers, customers, products, sales]:
                for downstream in [supplier_perf, product_perf]:
                    upstream >> downstream

            [supplier_perf, product_perf] >> customer_sales

        if prev_group:
            prev_group >> group
        prev_group = group

ingestion()
