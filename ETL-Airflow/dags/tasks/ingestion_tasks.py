# Import libraries
from airflow.decorators import task
from pyspark.sql import Row, SparkSession
import logging
from utils import get_spark_session, write_into_table, abort_session, APIClient

# Create a task that helps in ingesting the data into Suppliers
@task(task_id="ingest_data_into_suppliers")
def supplier_data_ingestion():
    
    # Create an object for the Suppliers API Client Class
    client = APIClient()

    # Get a spark session
    spark = get_spark_session()
    
    # Set the Suppliers value to fetch the data
    api = "suppliers"
    response = client.fetch_data(api)

    # Create a data frame from the response of the API
    suppliers_df = spark.createDataFrame(Row(**x) for x in response['data'])

    # Do the Transformations for the Suppliers Dataframe
    suppliers_df = suppliers_df\
        .withColumnRenamed(suppliers_df.columns[0], "supplier_id")\
        .withColumnRenamed(suppliers_df.columns[1], "supplier_name")\
        .withColumnRenamed(suppliers_df.columns[2], "contact_details")\
        .withColumnRenamed(suppliers_df.columns[3], "region")
    logging.info(f"Writing into table: {api}")

    # Load the data into the table
    write_into_table(api, suppliers_df, "raw", "overwrite")

    # Abort the session when Done.
    abort_session(spark)
    return f"{api} data ingested successfully!"

# Create a task that helps in ingesting the data into Customers
@task(task_id="ingest_data_into_customers")
def customer_data_ingestion():
    
    # Create an object for the Suppliers API Client Class
    client = APIClient()

    # Get a spark session
    spark = get_spark_session()
    
    # Set the Customers value to fetch the data
    api = "customer"
    response = client.fetch_data(api, True)

    # Create a data frame from the response of the API
    customer_df = spark.createDataFrame(Row(**x) for x in response['data'])
    
    # Do the transformations for the customers Dataframe
    customer_df = customer_df\
        .withColumnRenamed(customer_df.columns[0], "customer_id")\
        .withColumnRenamed(customer_df.columns[1], "name")\
        .withColumnRenamed(customer_df.columns[2], "city")\
        .withColumnRenamed(customer_df.columns[3], "email")\
        .withColumnRenamed(customer_df.columns[4], "phone_number")
    logging.info(f"Writing into table: {api}")

    # Load the data into the table
    write_into_table(api, customer_df, "raw", "overwrite")
    
    # Abort the session when Done
    abort_session(spark)
    return f"{api} data ingested successfully!"

# Create a task that helps in ingesting the data into Products
@task(task_id="ingest_data_into_products")
def products_data_ingestion():

    # Create an object for the Suppliers API Client Class
    client = APIClient()

    # Get a spark session
    spark = get_spark_session()
    
    # Set the Products value to fetch the data
    api = "products"
    response = client.fetch_data(api)
    
    # Create a data frame from the response of the API
    product_df = spark.createDataFrame(Row(**x) for x in response['data'])

    # Do the Transformation for the product Dataframe
    product_df = product_df\
        .withColumnRenamed(product_df.columns[0], "product_id")\
        .withColumnRenamed(product_df.columns[1], "product_name")\
        .withColumnRenamed(product_df.columns[2], "category")\
        .withColumnRenamed(product_df.columns[3], "price")\
        .withColumnRenamed(product_df.columns[4], "stock_quantity")\
        .withColumnRenamed(product_df.columns[5], "reorder_level")\
        .withColumnRenamed(product_df.columns[6], "supplier_id")
    logging.info(f"Writing into table: {api}")

    # Load the data into the table
    write_into_table(api, product_df, "raw", "overwrite")
    
    # Abort the session once Done
    abort_session(spark)
    return f"{api} data ingested successfully!"

# Create a task that helps the data in ingesting the data into sales
@task(task_id="ingest_data_into_sales")
def sales_data_ingestion():

    # Create a spark session with the hadoop configurations and also authentic credentials
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/jars/meta-morph-d-eng-pro-admin.json")

    # Create a data frame by reading the CSV from the Google Bucket
    sales_df = spark.read.csv('gs://meta-morph/20250330/sales_20250330.csv', header=True, inferSchema=True)
    
    api = "sales"
    logging.info("Reading the CSV File into dataframe...")
    # Do the Transformation for the Sales Dataframe
    sales_df = sales_df\
        .withColumnRenamed(sales_df.columns[0], "sale_id")\
        .withColumnRenamed(sales_df.columns[1], "customer_id")\
        .withColumnRenamed(sales_df.columns[2], "product_id")\
        .withColumnRenamed(sales_df.columns[3], "sale_date")\
        .withColumnRenamed(sales_df.columns[4], "quantity")\
        .withColumnRenamed(sales_df.columns[5], "discount")\
        .withColumnRenamed(sales_df.columns[6], "shipping_cost")\
        .withColumnRenamed(sales_df.columns[7], "order_status")\
        .withColumnRenamed(sales_df.columns[8], "payment_mode")
    logging.info(f"Writing into table: {api}")

    # Load the data into the Table
    write_into_table(api, sales_df, "raw", "overwrite")

    # abort the session after Done
    abort_session(spark)
    return f"{api} data ingested successfully!"
