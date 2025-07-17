# Import Libraries
from airflow.decorators import dag
from datetime import datetime
from tasks.adhoc.adhoc_env_test import test_environment
import os


# Create a Dag
@dag(
    dag_id="Prod_Adhoc_Job",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def ingestion():
    env = os.getenv("ENV", "dev")
    test_environment(env)


# Call the Ingestion Dag Function
ingestion()
