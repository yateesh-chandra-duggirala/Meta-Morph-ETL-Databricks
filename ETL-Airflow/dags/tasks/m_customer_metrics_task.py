# Import Libraries
from airflow.decorators import task
import logging
from tasks.utils import get_spark_session, write_into_table, abort_session, read_data, DuplicateChecker, DuplicateException, write_to_gcs
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a task that helps in Populating Suppliers Performance Table
@task(task_id="m_load_customer_metrics")
def customer_metrics_upsert():
    """
    Create a function to upsert into the Customer Metrics

    Returns: The Success message for the upsert

    Raises: Duplicate exception if any Duplicates are found..
    """

    # Get a spark session
    spark = get_spark_session()

    # Process the Node : SQ_Shortcut_To_Customers - reads data from Customer Table
    SQ_Shortcut_To_Customers = read_data(spark,"legacy.customers")
    SQ_Shortcut_To_Customers = SQ_Shortcut_To_Customers \
                                .select(
                                    col("customer_id"),
                                    col("name"),
                                    col("city"),
                                    col("email"),
                                    col("phone_number")
                                )
    logging.info(f"Data Frame : 'SQ_Shortcut_To_Customers' is built...")

    # Process the Node : SQ_Shortcut_To_Products - reads data from Products Table
    SQ_Shortcut_To_Products = read_data(spark,"legacy.products")
    SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                .select(
                                    col("product_id"),
                                    col("selling_price")
                                )
    logging.info(f"Data Frame : 'SQ_Shortcut_To_Products' is built...")

    # Process the Node : SQ_Shortcut_To_Sales - reads data from Sales Table
    SQ_Shortcut_To_Sales = read_data(spark,"legacy.sales")
    SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales \
                                .select(
                                    col("sale_id"),
                                    col("product_id"),
                                    col("customer_id"),
                                    col("order_status"),
                                    col("payment_mode"),
                                    col("shipping_cost"),
                                    col("quantity"),
                                    col("discount"),
                                    col("sale_date")
                                )
    logging.info(f"Data Frame : 'SQ_Shortcut_To_Sales' is built...")

    JNR_Master = SQ_Shortcut_To_Customers \
                    .join(
                        SQ_Shortcut_To_Sales,
                        SQ_Shortcut_To_Sales.customer_id == SQ_Shortcut_To_Customers.customer_id,
                        "left"
                    ) \
                    .select(
                        SQ_Shortcut_To_Customers.customer_id,
                        SQ_Shortcut_To_Customers.name,
                        SQ_Shortcut_To_Sales.product_id,
                        SQ_Shortcut_To_Sales.order_status,
                        SQ_Shortcut_To_Sales.payment_mode,
                        SQ_Shortcut_To_Sales.shipping_cost,
                        SQ_Shortcut_To_Sales.quantity,
                        SQ_Shortcut_To_Sales.discount,
                        SQ_Shortcut_To_Sales.sale_date,
                        SQ_Shortcut_To_Customers.city,
                        SQ_Shortcut_To_Customers.email,
                        SQ_Shortcut_To_Customers.phone_number,
                    )

    AGG_TRANS = JNR_Master \
                    .groupBy(
                        "customer_id", "name", "city", "email", "phone_number"
                    ) \
                    .agg(
                        sum("quantity").alias("TOTAL_ORDERS"),
                        max("sale_date").alias("LAST_PURCHASE_DATE"),
                        min("sale_date").alias("FIRST_PURCHASE_DATE"),
                        sum("shipping_cost").alias("TOTAL_SHIPPING_COST")
                    )
    
    Shortcut_To_Customer_Metrics = AGG_TRANS \
                                    .select(
                                        col("customer_id").alias("CUSTOMER_ID"),
                                        col("name").alias("CUSTOMER_NAME"),
                                        col("TOTAL_ORDERS"),
                                        col("FIRST_PURCHASE_DATE"),
                                        col("LAST_PURCHASE_DATE"),
                                        col("TOTAL_SHIPPING_COST"),
                                        col("city").alias("CITY"),
                                        col("email").alias("EMAIL"),
                                        col("phone_number").alias("PHONE_NUMBER")
                                    )

    try :

        # Implement the Duplicate checker
        # JNR_Master.show()
        chk = DuplicateChecker()
        chk.has_duplicates(Shortcut_To_Customer_Metrics, ['CUSTOMER_ID'])
        
        # Load the Data into Parquet File
        # write_to_gcs(Shortcut_To_Suppliers_Performance_tgt, "supplier_performance")

        # Load the data into the table
        write_into_table("customer_metrics_stg", Shortcut_To_Customer_Metrics, "staging", "overwrite")

    except DuplicateException as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise
    
    finally :

        # Abort the session when Done.
        abort_session(spark)

    return f"supplier_performance data ingested successfully!"