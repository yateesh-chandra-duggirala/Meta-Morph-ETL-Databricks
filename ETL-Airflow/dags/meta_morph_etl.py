# Import Libraries
from airflow.decorators import dag
from datetime import datetime
from tasks.ingestion_tasks import (
    supplier_data_ingestion,
    customer_data_ingestion,
    products_data_ingestion,
    sales_data_ingestion
)
from tasks.m_supplier_performance_task import suppliers_performance_ingestion
from tasks.m_product_performance_task import product_performance_ingestion

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

    # Make a function call for loading the Suppliers Performance task
    supplier_performance = suppliers_performance_ingestion()

    # Make a function call for loading the Product Performance Task
    product_performance = product_performance_ingestion()

    # Set the Tasks Dependency
    [suppliers, customers, products, sales] >> supplier_performance >> product_performance

# Call the Ingestion Dag Function
ingestion()