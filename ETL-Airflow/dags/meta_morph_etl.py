# Import Libraries
from airflow.decorators import dag
from datetime import datetime
from tasks.ingestion_tasks import (
    supplier_data_ingestion,
    customer_data_ingestion,
    products_data_ingestion,
    sales_data_ingestion
)

# Create a Dag
@dag(
    dag_id="meta_morph_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def ingestion():

    # Make a function call for the Suppliers task
    suppliers = supplier_data_ingestion()

    # Make a function call for the Customer task
    customers = customer_data_ingestion()
    
    # Make a function call for the Products task
    products = products_data_ingestion()

    # Make a function call for the Sales task
    sales = sales_data_ingestion()

    # Set the Tasks Dependency
    [suppliers, customers, products, sales]

# Call the Ingestion Dag Function
ingestion()