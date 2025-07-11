# Import Libraries
from airflow.decorators import dag
from datetime import datetime
from tasks.adhoc.adhoc_MM_145_20250711 import (
    load_prod_supplier_performance,
    push_data_to_reporting
)
from tasks.utils import get_spark_session, abort_session
from tasks.m_customer_sales_report_task import customer_sales_report_ingestion


# Create a Dag
@dag(
    dag_id="Prod_Adhoc_Job",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def ingestion():

    try:
        spark = get_spark_session()

        prod_supplier_performance_task = load_prod_supplier_performance(spark)
        send_to_reporting_task = push_data_to_reporting(spark)
        csr_task = customer_sales_report_ingestion()

        prod_supplier_performance_task >> [send_to_reporting_task, csr_task]

    except Exception as e:
        raise e

    finally:
        abort_session(spark)

    return "Adhoc job completed successfully.."


# Call the Ingestion Dag Function
ingestion()
