from airflow.decorators import dag, task
from datetime import datetime
from pyspark.sql import Row, SparkSession
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
    
    @task(task_id="ingest_data_into_sales")
    def gcs_buck(api):
        # Create Spark session with required configurations
        spark = SparkSession.builder.appName("GCS_to_Postgres") \
            .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .getOrCreate()

        logging.info("Created Spark Session with GCS support.")

        # Set authentication for Google Cloud Storage
        spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/jars/meta-morph-d-eng-pro-admin.json")
        
        logging.info("Configured JSON authentication.")

        # Read CSV from GCS
        sales_df = spark.read.csv('gs://meta-morph/20250323/sales_20250323.csv', header=True, inferSchema=True)
        load_into_table(api, sales_df)
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

    sales_task = gcs_buck("sales")

    [suppliers_task , customers_task , products_task, sales_task]

ingestion()