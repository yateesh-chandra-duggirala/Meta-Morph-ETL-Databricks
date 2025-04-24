# Import Libraries
from airflow.decorators import task
import logging
from utils import get_spark_session, write_into_table, abort_session, read_data
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a task that helps in ingesting the data into Suppliers
@task(task_id="m_load_suppliers_performance")
def customer_sales_report_ingestion():

    # Get a spark session
    spark = get_spark_session()

    # Process the Node : SQ_Shortcut_To_Products - reads data from Products Table
    SQ_Shortcut_To_Products = read_data(spark,"raw.products")
    SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                .select(
                                    col("product_id"),
                                    col("product_name"),
                                    col("category"),
                                    col("price")
                                )

    # Process the Node : SQ_Shortcut_To_Sales - reads data from Sales Table
    SQ_Shortcut_To_Sales = read_data(spark,"raw.sales")
    SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales \
                                .select(
                                    col("sale_id"),
                                    col("customer_id"),
                                    col("product_id"),
                                    col("order_status"),
                                    col("quantity"),
                                    col("discount"),
                                    col("sale_date"),
                                    col("shipping_cost")
                                )

    # Process the Node : SQ_Shortcut_To_Customers - reads data from Customers Table
    SQ_Shortcut_To_Customers = read_data(spark,"raw.customers")
    SQ_Shortcut_To_Customers = SQ_Shortcut_To_Customers \
                                .select(
                                    col("customer_id"),
                                    col("name"),
                                    col("city")
                                )

    # Process the Node : JNR_Sales_Customer - joins the 2 nodes SQ_Shortcut_To_Sales and SQ_Shortcut_To_Customers
    JNR_Sales_Customer = SQ_Shortcut_To_Sales \
                            .join(
                                SQ_Shortcut_To_Customers, 
                                (SQ_Shortcut_To_Sales.customer_id == SQ_Shortcut_To_Customers.customer_id) &
                                (SQ_Shortcut_To_Sales.order_status != "Cancelled"), 
                                'left'
                            ) \
                            .select(
                                SQ_Shortcut_To_Sales.sale_id,
                                SQ_Shortcut_To_Sales.product_id,
                                SQ_Shortcut_To_Sales.order_status,
                                SQ_Shortcut_To_Sales.quantity,
                                SQ_Shortcut_To_Sales.discount,
                                SQ_Shortcut_To_Sales.sale_date,
                                SQ_Shortcut_To_Sales.shipping_cost,
                                SQ_Shortcut_To_Customers.customer_id,
                                SQ_Shortcut_To_Customers.name,
                                SQ_Shortcut_To_Customers.city
                            )

    # Process the Node : JNR_Master - joins the 2 nodes and JNR_Sales_Customer and SQ_Shortcut_To_Products
    JNR_Master = JNR_Sales_Customer \
                            .join(
                                SQ_Shortcut_To_Products,
                                (JNR_Sales_Customer.product_id == SQ_Shortcut_To_Products.product_id) & 
                                (JNR_Sales_Customer.order_status != "Cancelled"),
                                "left"
                            ) \
                            .select(
                                JNR_Sales_Customer.sale_id,
                                JNR_Sales_Customer.product_id,
                                JNR_Sales_Customer.order_status,
                                JNR_Sales_Customer.quantity,
                                JNR_Sales_Customer.discount,
                                JNR_Sales_Customer.sale_date,
                                JNR_Sales_Customer.shipping_cost,
                                JNR_Sales_Customer.customer_id,
                                JNR_Sales_Customer.name,
                                JNR_Sales_Customer.city,
                                SQ_Shortcut_To_Products.product_id,
                                SQ_Shortcut_To_Products.product_name,
                                SQ_Shortcut_To_Products.category,
                                SQ_Shortcut_To_Products.price
                            )

    # Abort the session when Done.
    abort_session(spark)
    return f"supplier_performance data ingested successfully!"