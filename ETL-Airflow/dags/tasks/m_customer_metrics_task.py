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

    # Process the Node : JNR_Master - Joins Customers and Sales Dataframe.
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
    logging.info(f"Data Frame : 'JNR_Master' is built...")

    # Process the Node : JNR_Full - Joining JNR_Master and Products Dataframe
    JNR_Full = JNR_Master \
                    .join(
                        SQ_Shortcut_To_Products,
                        SQ_Shortcut_To_Products.product_id == JNR_Master.product_id,
                        "left"
                    ) \
                    .select(
                        JNR_Master.customer_id,
                        JNR_Master.name,
                        JNR_Master.order_status,
                        JNR_Master.payment_mode,
                        JNR_Master.shipping_cost,
                        JNR_Master.quantity,
                        JNR_Master.discount,
                        JNR_Master.sale_date,
                        JNR_Master.city,
                        JNR_Master.email,
                        JNR_Master.phone_number,
                        SQ_Shortcut_To_Products.product_id,
                        SQ_Shortcut_To_Products.selling_price                        
                    )
    logging.info(f"Data Frame : 'JNR_Full' is built...")

    # Process the Node : AGG_TRANS - calcluating the Aggregates required.
    AGG_TRANS = JNR_Full \
                    .groupBy(
                        "customer_id", "name", "city", "email", "phone_number", "payment_mode"
                    ) \
                    .agg(
                        sum("quantity").alias("TOTAL_ORDERS"),
                        max("sale_date").alias("LAST_PURCHASE_DATE"),
                        min("sale_date").alias("FIRST_PURCHASE_DATE"),
                        sum("shipping_cost").alias("TOTAL_SHIPPING_COST"),
                        sum(col("quantity") * col("selling_price")).alias("EXPENDITURE"),
                        sum(col("quantity") * col("selling_price") * col("discount") / lit(100)).alias("TOTAL_AMOUNT_SAVINGS"),
                        sum(when(col("order_status") == 'Delivered', lit(1)).otherwise(lit(0))).alias("DELIVERED_ORDERS_COUNT"),
                        sum(when(col("order_status") == 'Cancelled', lit(1)).otherwise(lit(0))).alias("CANCELLED_ORDERS_COUNT"),
                    )\
                    .withColumn("AVERAGE_ORDER_VALUE", col("EXPENDITURE") / col("TOTAL_ORDERS")) \
                    .withColumn("ACTIVE_CUSTOMER_FLAG",
                                when(col("LAST_PURCHASE_DATE") >= current_date() - 4, lit("TRUE"))
                                .otherwise(lit("FALSE"))) \
                    .withColumn("LOAD_TIMESTAMP", current_timestamp()) \
                    .withColumn("UPDATE_TIMESTAMP", current_timestamp())
    logging.info("Data Frame : 'AGG_TRANS' is built...")

    # Assign Rank based on the Payment Mode.
    window_spec = Window.partitionBy('customer_id').orderBy(desc('payment_mode'))
    RNK_Payment_Mode = AGG_TRANS.withColumn("rnk", row_number().over(window_spec)) \
                            .filter(col("rnk") == 1) \
                            .drop(col("rnk"))

    # Process the Node : Shortcut_To_Customer_Metrics - Target Dataframe.
    Shortcut_To_Customer_Metrics = RNK_Payment_Mode \
                                    .select(
                                        col("customer_id").alias("CUSTOMER_ID"),
                                        col("name").alias("CUSTOMER_NAME"),
                                        col("TOTAL_ORDERS"),
                                        col("TOTAL_AMOUNT_SAVINGS"),
                                        col("TOTAL_SHIPPING_COST"),
                                        col("EXPENDITURE"),
                                        col("AVERAGE_ORDER_VALUE"),
                                        col("FIRST_PURCHASE_DATE"),
                                        col("LAST_PURCHASE_DATE"),
                                        col("payment_mode").alias("MOST_USED_PAYMENT_MODE"),
                                        col("DELIVERED_ORDERS_COUNT"),
                                        col("CANCELLED_ORDERS_COUNT"),
                                        col("ACTIVE_CUSTOMER_FLAG"),
                                        col("city").alias("CITY"),
                                        col("email").alias("EMAIL"),
                                        col("phone_number").alias("PHONE_NUMBER"),
                                        col("LOAD_TIMESTAMP"),
                                        col("UPDATE_TIMESTAMP")
                                    )
    logging.info(f"Data Frame : 'Shortcut_To_Customer_Metrics' is built...")

    try :

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(Shortcut_To_Customer_Metrics, ['CUSTOMER_ID'])

        # Load the Data into Parquet File
        # write_to_gcs(Shortcut_To_Customer_Metrics, "customer_metrics_stg")

        # Load the data into the table
        write_into_table("customer_metrics_stg", Shortcut_To_Customer_Metrics, "staging", "overwrite")

    except DuplicateException as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise
    
    finally :

        # Abort the session when Done.
        abort_session(spark)

    return f"customer_metrics data ingested successfully!"