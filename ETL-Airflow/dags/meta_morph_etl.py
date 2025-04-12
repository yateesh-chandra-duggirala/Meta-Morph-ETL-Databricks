# Import Libraries
from airflow.decorators import dag
from datetime import datetime
from utils import APIClient
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
    #  Create an object for the APIClient Class
    client = APIClient()

    # Make a function call for the Suppliers API
    api = "suppliers"
    response = client.fetch_data(api)
    suppliers = supplier_data_ingestion(api, response)

    # Make a function call for the Customer API
    api = "customers"
    response = client.fetch_data(api, True)
    customers = customer_data_ingestion(api, response)
    
    # Make a function call for the Products API
    api = "products"
    response = client.fetch_data(api)
    products = products_data_ingestion(api, response)

    # Make a function call for the Sales API
    sales = sales_data_ingestion("sales")

    # Set the Task Dependency
    [suppliers, customers, products, sales]

# Call the Ingestion Dag Function
ingestion()