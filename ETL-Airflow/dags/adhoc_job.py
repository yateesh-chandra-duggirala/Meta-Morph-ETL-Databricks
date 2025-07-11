# Import Libraries
from airflow.decorators import dag
from datetime import datetime
from tasks.adhoc.adhoc_MM_148_20250711 import push_data_to_reporting


# Create a Dag
@dag(
    dag_id="Prod_Adhoc_Job",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def ingestion():

    push_data_to_reporting()


# Call the Ingestion Dag Function
ingestion()
