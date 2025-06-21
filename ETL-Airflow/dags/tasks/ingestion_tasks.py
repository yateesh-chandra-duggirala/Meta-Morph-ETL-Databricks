# Import libraries
from airflow.decorators import task
from pyspark.sql import Row
from pyspark.sql.functions import current_date, col
import logging
from tasks.utils import get_spark_session, write_into_table, abort_session, APIClient, DuplicateChecker, DuplicateException, write_to_gcs

# Create a task that helps in ingesting the data into Suppliers
@task(task_id="m_ingest_data_into_suppliers")
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
    logging.info("Data Frame : 'suppliers_df' is built")

    # Do the Transformations for the Suppliers Dataframe
    suppliers_df = suppliers_df \
                    .withColumnRenamed(suppliers_df.columns[0], "SUPPLIER_ID") \
                    .withColumnRenamed(suppliers_df.columns[1], "SUPPLIER_NAME") \
                    .withColumnRenamed(suppliers_df.columns[2], "CONTACT_DETAILS") \
                    .withColumnRenamed(suppliers_df.columns[3], "REGION")
    logging.info("Data Frame : Transformed 'suppliers_df' is built")

    suppliers_df_lgcy = suppliers_df \
                            .withColumn("DAY_DT", current_date()) \
                            .select(
                                col("DAY_DT"),
                                col("SUPPLIER_ID"),
                                col("SUPPLIER_NAME"),
                                col("CONTACT_DETAILS"),
                                col("REGION")
                            )
    logging.info("Data Frame : 'suppliers_df_lgcy' is built")

    try :

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(suppliers_df, ['SUPPLIER_ID'])

        # Write the Data into GCS Reporting
        write_to_gcs(suppliers_df_lgcy, api)

        # Load the data into the tables
        write_into_table("suppliers_pre", suppliers_df, "raw", "overwrite")
        write_into_table(api, suppliers_df_lgcy, "legacy", "append")

    except DuplicateException as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise

    finally :

        # Abort the session when Done.
        abort_session(spark)

    return f"{api} data ingested successfully!"

# Create a task that helps in ingesting the data into Customers
@task(task_id="m_ingest_data_into_customers")
def customer_data_ingestion():
    
    # Create an object for the Suppliers API Client Class
    client = APIClient()

    # Get a spark session
    spark = get_spark_session()
    
    # Set the Customers value to fetch the data
    api = "customers"
    response = client.fetch_data(api, True)

    # Create a data frame from the response of the API
    customer_df = spark.createDataFrame(Row(**x) for x in response['data'])
    logging.info("Data Frame : 'customer_df' is built")
    
    # Do the transformations for the customers Dataframe
    customer_df = customer_df \
                    .withColumnRenamed(customer_df.columns[0], "CUSTOMER_ID") \
                    .withColumnRenamed(customer_df.columns[1], "NAME") \
                    .withColumnRenamed(customer_df.columns[2], "CITY") \
                    .withColumnRenamed(customer_df.columns[3], "EMAIL") \
                    .withColumnRenamed(customer_df.columns[4], "PHONE_NUMBER")
    logging.info("Data Frame : Transformed 'customer_df' is built")

    customer_df_lgcy = customer_df \
                            .withColumn("DAY_DT", current_date()) \
                            .select(
                                col("DAY_DT"),
                                col("CUSTOMER_ID"),
                                col("NAME"),
                                col("CITY"),
                                col("EMAIL"),
                                col("PHONE_NUMBER")
                            )
    logging.info("Data Frame : 'customer_df_lgcy' is built")

    try :

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(customer_df, ['CUSTOMER_ID'])

        # Write the Data into GCS Reporting
        write_to_gcs(customer_df_lgcy, api)

        # Load the data into the tables
        write_into_table("customers_pre", customer_df, "raw", "overwrite")
        write_into_table(api, customer_df_lgcy, "legacy", "append")

    except DuplicateException as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise

    finally :

        # Abort the session when Done.
        abort_session(spark)

    return f"{api} data ingested successfully!"

# Create a task that helps in ingesting the data into Products
@task(task_id="m_ingest_data_into_products")
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
    logging.info("Data Frame : 'product_df' is built")

    # Do the Transformation for the product Dataframe
    product_df = product_df \
                    .withColumnRenamed(product_df.columns[0], "PRODUCT_ID") \
                    .withColumnRenamed(product_df.columns[1], "PRODUCT_NAME") \
                    .withColumnRenamed(product_df.columns[2], "CATEGORY") \
                    .withColumnRenamed(product_df.columns[3], "SELLING_PRICE") \
                    .withColumnRenamed(product_df.columns[4], "COST_PRICE") \
                    .withColumnRenamed(product_df.columns[5], "STOCK_QUANTITY") \
                    .withColumnRenamed(product_df.columns[6], "REORDER_LEVEL") \
                    .withColumnRenamed(product_df.columns[7], "SUPPLIER_ID")
    logging.info("Data Frame : Transformed 'product_df' is built")

    product_df_lgcy = product_df \
                            .withColumn("DAY_DT", current_date()) \
                            .select(
                                col("DAY_DT"),
                                col("PRODUCT_ID"),
                                col("PRODUCT_NAME"),
                                col("CATEGORY"),
                                col("SELLING_PRICE"),
                                col("COST_PRICE"),
                                col("STOCK_QUANTITY"),
                                col("REORDER_LEVEL"),
                                col("SUPPLIER_ID")
                            )
    logging.info("Data Frame : 'product_df_lgcy' is built")

    try :

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(product_df, ['PRODUCT_ID'])

        # Write the Data into GCS Reporting
        write_to_gcs(product_df_lgcy, api)

        # Load the data into the tables
        write_into_table("products_pre", product_df, "raw", "overwrite")
        write_into_table(api, product_df_lgcy, "legacy", "append")

    except DuplicateException as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise

    finally :

        # Abort the session when Done.
        abort_session(spark)

    return f"{api} data ingested successfully!"

# Create a task that helps the data in ingesting the data into sales
@task(task_id="m_ingest_data_into_sales")
def sales_data_ingestion():

    # today = datetime.now().strftime("%Y%m%d")
    today = "20250328"

    # Create a spark session with the hadoop configurations and also authentic credentials
    spark = get_spark_session()

    # Create a data frame by reading the CSV from the Google Bucket
    sales_df = spark.read.csv(f'gs://meta-morph-flow/{today}/sales_{today}.csv', header=True, inferSchema=True)
    logging.info("Data Frame : 'sales_df' is built")
    
    api = "sales"
    logging.info("Reading the CSV File into dataframe...")
    # Do the Transformation for the Sales Dataframe
    sales_df = sales_df \
                .withColumnRenamed(sales_df.columns[0], "SALE_ID") \
                .withColumnRenamed(sales_df.columns[1], "CUSTOMER_ID") \
                .withColumnRenamed(sales_df.columns[2], "PRODUCT_ID") \
                .withColumnRenamed(sales_df.columns[3], "SALE_DATE") \
                .withColumnRenamed(sales_df.columns[4], "QUANTITY") \
                .withColumnRenamed(sales_df.columns[5], "DISCOUNT") \
                .withColumnRenamed(sales_df.columns[6], "SHIPPING_COST") \
                .withColumnRenamed(sales_df.columns[7], "ORDER_STATUS") \
                .withColumnRenamed(sales_df.columns[8], "PAYMENT_MODE")
    logging.info("Data Frame : Transformed 'sales_df' is built")

    sales_df_lgcy = sales_df \
                            .withColumn("DAY_DT", current_date()) \
                            .select(
                                col("DAY_DT"),
                                col("SALE_ID"),
                                col("CUSTOMER_ID"),
                                col("PRODUCT_ID"),
                                col("SALE_DATE"),
                                col("QUANTITY"),
                                col("DISCOUNT"),
                                col("SHIPPING_COST"),
                                col("ORDER_STATUS"),
                                col("PAYMENT_MODE")
                            )
    logging.info("Data Frame : 'sales_df_lgcy' is built")

    try :

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(sales_df, ['SALE_ID'])

        # Write the Data into GCS Reporting
        write_to_gcs(sales_df_lgcy, api)

        # Load the data into the table
        write_into_table("sales_pre", sales_df, "raw", "overwrite")
        write_into_table(api, sales_df_lgcy, "legacy", "append")

    except DuplicateException as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise

    finally :

        # Abort the session when Done.
        abort_session(spark)

    return f"{api} data ingested successfully!"
