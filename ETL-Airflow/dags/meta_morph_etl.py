from airflow.decorators import dag, task
from datetime import datetime
from pyspark.sql import Row
from utils import APIClient,get_spark_session, load_into_table, abort_session
import logging
    
@dag(
    dag_id = "meta_morph_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def ingestion():
        
    @task(task_id = "ingest_data_into_suppliers")
    def supplier_data_ingestion(api, response):
        spark = get_spark_session()
        supplier_df = spark.createDataFrame(Row(**x) for x in response['data'])
        logging.info(f"Data Frame Loaded.. Writing into Table : {api}")
        load_into_table(api, supplier_df)
        abort_session(spark)
        return f"ETL Pipeline ingested {api} data successfully..!"
    
    @task(task_id = "ingest_data_into_customers")
    def customer_data_ingestion(api, response):
        spark = get_spark_session()
        customer_df = spark.createDataFrame(Row(**x) for x in response['data'])
        logging.info(f"Data Frame Loaded.. Writing into Table : {api}")
        load_into_table(api, customer_df)
        abort_session(spark)
        return f"ETL Pipeline ingested {api} data successfully..!"
    
    @task(task_id = "ingest_data_into_products")
    def products_data_ingestion(api, response):
        spark = get_spark_session()
        products_df = spark.createDataFrame(Row(**x) for x in response['data'])
        logging.info(f"Data Frame Loaded.. Writing into Table : {api}")
        load_into_table(api, products_df)
        abort_session(spark)
        return f"ETL Pipeline ingested {api} data successfully..!"
    
    client = APIClient()
    api_value = "suppliers"
    response = client.fetch_data(api_value)
    suppliers_task = supplier_data_ingestion(api_value, response)

    api_value = "customers"
    response = client.fetch_data(api_value)
    customers_task = customer_data_ingestion(api_value, response)

    api_value = "products"
    response = client.fetch_data(api_value)
    products_task = products_data_ingestion(api_value, response)

    [suppliers_task , customers_task , products_task]

ingestion()