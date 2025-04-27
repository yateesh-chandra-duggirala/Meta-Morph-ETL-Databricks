# Import Libraries
from airflow.decorators import task
import logging
from utils import get_spark_session, write_into_table, abort_session, read_data
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a task that helps in ingesting the data into Suppliers
@task(task_id="m_load_suppliers_performance")
def suppliers_performance_ingestion():

    # Get a spark session
    spark = get_spark_session()

    # Process the Node : SQ_Shortcut_To_Suppliers - reads data from Suppliers Table
    SQ_Shortcut_To_Suppliers = read_data(spark,"raw.suppliers")
    SQ_Shortcut_To_Suppliers = SQ_Shortcut_To_Suppliers \
                                .select(
                                    col("supplier_id"),
                                    col("supplier_name")
                                )

    # Process the Node : SQ_Shortcut_To_Products - reads data from Products Table
    SQ_Shortcut_To_Products = read_data(spark,"raw.products")
    SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                .select(
                                    col("product_id"),
                                    col("product_name"),
                                    col("supplier_id"),
                                    col("price")
                                )

    # Process the Node : SQ_Shortcut_To_Sales - reads data from Sales Table
    SQ_Shortcut_To_Sales = read_data(spark,"raw.sales")
    SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales \
                                .select(
                                    col("sale_id"),
                                    col("product_id"),
                                    col("order_status"),
                                    col("quantity"),
                                    col("discount")
                                )

    # Process the Node : JNR_Supplier_Products - joins the 2 nodes SQ_Shortcut_To_Suppliers and SQ_Shortcut_To_Products
    JNR_Supplier_Products = SQ_Shortcut_To_Suppliers \
                            .join(
                                SQ_Shortcut_To_Products, 
                                trim(SQ_Shortcut_To_Suppliers.supplier_id) == trim(SQ_Shortcut_To_Products.supplier_id), 
                                'left'
                            ) \
                            .select(
                                SQ_Shortcut_To_Suppliers.supplier_id,
                                SQ_Shortcut_To_Suppliers.supplier_name,
                                SQ_Shortcut_To_Products.product_id,
                                SQ_Shortcut_To_Products.product_name,
                                SQ_Shortcut_To_Products.price
                            )

    # Process the Node : JNR_Master - joins the 2 nodes and JNR_Supplier_Products and SQ_Shortcut_To_Sales
    JNR_Master = JNR_Supplier_Products \
                            .join(
                                SQ_Shortcut_To_Sales,
                                (SQ_Shortcut_To_Sales.product_id == JNR_Supplier_Products.product_id) & 
                                (SQ_Shortcut_To_Sales.order_status != "Cancelled"),
                                "left"
                            ) \
                            .select(
                                JNR_Supplier_Products.supplier_id,
                                JNR_Supplier_Products.supplier_name,
                                JNR_Supplier_Products.product_name,
                                JNR_Supplier_Products.price,
                                SQ_Shortcut_To_Sales.sale_id,
                                SQ_Shortcut_To_Sales.order_status,
                                SQ_Shortcut_To_Sales.quantity,
                                SQ_Shortcut_To_Sales.discount
                            )
    
    # Process the Node : AGG_TRANS - Calculate the aggregates that are needed for the target columns
    AGG_TRANS = JNR_Master \
                    .groupBy(["supplier_id", "supplier_name", "product_name"]) \
                    .agg(
                        coalesce(
                            round(sum(
                            (col("price") - (col("price") * col("discount") / 100.0)) * col("quantity")
                            ),2), lit(0.0)
                        ).alias("agg_total_revenue"),

                        count(col("sale_id")).alias("agg_total_products_sold"),
                        
                        coalesce(
                            round(sum(
                                col("quantity")
                            ), 2), lit(0)
                        ).alias("agg_total_stocks_sold")
                    )

    # Assigns a rank to each supplier based on their total stocks sold (highest first), within each supplier group
    window_spec = Window.partitionBy(col("supplier_id")).orderBy(desc("agg_total_stocks_sold"))
    Shortcut_To_Suppliers_Performance_tgt = AGG_TRANS.withColumn("rnk", row_number().over(window_spec))

    # Assign a Unique Performance ID for the records
    Shortcut_To_Suppliers_Performance_tgt = Shortcut_To_Suppliers_Performance_tgt \
                                            .filter("rnk = 1") \
                                            .drop(col("rnk")) \
                                            .withColumn("performance_id", monotonically_increasing_id() + 1) \
                                            .withColumn("day_dt", current_date())

    # Process the Node : Shortcut_To_Suppliers_Performance_tgt - The Target desired table
    Shortcut_To_Suppliers_Performance_tgt = Shortcut_To_Suppliers_Performance_tgt \
                                                .select(
                                                        col("day_dt").alias("DAY_DT"),
                                                        col("performance_id").alias("PERFORMANCE_ID"),
                                                        col("supplier_id").alias("SUPPLIER_ID"),
                                                        col("supplier_name").alias("SUPPLIER_NAME"),
                                                        col("agg_total_revenue").alias("TOTAL_REVENUE"),
                                                        col("agg_total_products_sold").alias("TOTAL_PRODUCTS_SOLD"),
                                                        col("agg_total_stocks_sold").alias("TOTAL_STOCKS_SOLD"),
                                                        col("product_name").alias("TOP_SELLING_PRODUCT")
                                                )
    logging.info("Data Frame : 'Shortcut_To_Suppliers_Performance_tgt' is built")

    logging.info("Authenticating to GCS to load the data into parquet file..")
    Shortcut_To_Suppliers_Performance_tgt.write.mode("append").parquet("gs://meta-reporting-data/supplier_performance.parquet")
    logging.info(f"Loaded into Parquet File : supplier_performance")

    # Load the data into the table
    write_into_table("supplier_performance", Shortcut_To_Suppliers_Performance_tgt, "legacy", "append")

    # Abort the session when Done.
    abort_session(spark)
    return f"supplier_performance data ingested successfully!"