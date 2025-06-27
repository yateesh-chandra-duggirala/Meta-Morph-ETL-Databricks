# Import Libraries
from airflow.decorators import task
import logging
from tasks.utils import get_spark_session, write_into_table, abort_session, read_data, DuplicateChecker, DuplicateException, write_to_gcs
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a task that helps in Populating Suppliers Performance Table
@task(task_id="m_load_suppliers_performance")
def suppliers_performance_ingestion():
    """
    Create a function to load the Suppliers Performance

    Returns: The Success message for the load

    Raises: Duplicate exception if any Duplicates are found..
    """

    # Get a spark session
    spark = get_spark_session()

    # Process the Node : SQ_Shortcut_To_Suppliers - reads data from Suppliers Table
    SQ_Shortcut_To_Suppliers = read_data(spark,"raw.suppliers_pre")
    SQ_Shortcut_To_Suppliers = SQ_Shortcut_To_Suppliers \
                                .select(
                                    col("supplier_id"),
                                    col("supplier_name")
                                )
    logging.info(f"Data Frame : 'SQ_Shortcut_To_Suppliers' is built...")

    # Process the Node : SQ_Shortcut_To_Products - reads data from Products Table
    SQ_Shortcut_To_Products = read_data(spark,"raw.products_pre")
    SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                .select(
                                    col("product_id"),
                                    col("product_name"),
                                    col("supplier_id"),
                                    col("selling_price")
                                )
    logging.info(f"Data Frame : 'SQ_Shortcut_To_Products' is built...")

    # Process the Node : SQ_Shortcut_To_Sales - reads data from Sales Table
    SQ_Shortcut_To_Sales = read_data(spark,"raw.sales_pre")
    SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales \
                                .select(
                                    col("sale_id"),
                                    col("product_id"),
                                    col("order_status"),
                                    col("quantity"),
                                    col("discount")
                                )
    logging.info(f"Data Frame : 'SQ_Shortcut_To_Sales' is built...")

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
                                SQ_Shortcut_To_Products.selling_price
                            )
    logging.info(f"Data Frame : 'JNR_Supplier_Products' is built...")

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
                                JNR_Supplier_Products.selling_price,
                                SQ_Shortcut_To_Sales.sale_id,
                                SQ_Shortcut_To_Sales.order_status,
                                SQ_Shortcut_To_Sales.quantity,
                                SQ_Shortcut_To_Sales.discount
                            )
    logging.info(f"Data Frame : 'JNR_Master' is built...")
    
    # Process the Node : AGG_TRANS - Calculate the aggregates that are needed for the target columns
    AGG_TRANS = JNR_Master \
                    .groupBy(
                        ["supplier_id", "supplier_name", "product_name"]
                    ) \
                    .agg(
                        coalesce(
                            round(sum(
                            (col("selling_price") - (col("selling_price") * col("discount") / 100.0)) * col("quantity")
                            ),2), lit(0.0)
                        ).alias("agg_total_revenue"),

                        count(col("sale_id")).alias("agg_total_products_sold"),
                        
                        coalesce(
                            round(sum(
                                col("quantity")
                            ), 2), lit(0)
                        ).alias("agg_total_stocks_sold")
                    )
    logging.info(f"Data Frame : 'AGG_TRANS' is built...")

    # Process the Node : EXP_Suppliers_Performance - Assigns rank
    window_spec = Window.partitionBy(col("supplier_id")).orderBy(desc("agg_total_stocks_sold"))
    EXP_Suppliers_Performance = AGG_TRANS \
                                    .withColumn(
                                        "rnk", row_number().over(window_spec)
                                    ) \
                                    .filter("rnk = 1") \
                                    .withColumn(
                                        "product_name",
                                        when(
                                            col("agg_total_revenue") > 0
                                            , col("product_name")
                                        ) \
                                        .otherwise(lit(None).cast("string"))
                                    ) \
                                    .drop(col("rnk")) \
                                    .withColumn("day_dt", current_date())
    logging.info("Data Frame : 'EXP_Suppliers_Performance' is built...")

    # Process the Node : Shortcut_To_Suppliers_Performance_tgt - The Target desired table
    Shortcut_To_Suppliers_Performance_tgt = EXP_Suppliers_Performance \
                                                .select(
                                                        col("day_dt").alias("DAY_DT"),
                                                        col("supplier_id").alias("SUPPLIER_ID"),
                                                        col("supplier_name").alias("SUPPLIER_NAME"),
                                                        col("agg_total_revenue").alias("TOTAL_REVENUE"),
                                                        col("agg_total_products_sold").alias("TOTAL_PRODUCTS_SOLD"),
                                                        col("agg_total_stocks_sold").alias("TOTAL_STOCK_SOLD"),
                                                        col("product_name").alias("TOP_SELLING_PRODUCT")
                                                )
    logging.info("Data Frame : 'Shortcut_To_Suppliers_Performance_tgt' is built...")

    try :

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(Shortcut_To_Suppliers_Performance_tgt, ['DAY_DT','SUPPLIER_ID'])
        
        # Load the Data into Parquet File
        write_to_gcs(Shortcut_To_Suppliers_Performance_tgt, "supplier_performance")

        # Load the data into the table
        write_into_table("supplier_performance", Shortcut_To_Suppliers_Performance_tgt, "legacy", "append")

    except DuplicateException as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise
    
    finally :

        # Abort the session when Done.
        abort_session(spark)

    return f"supplier_performance data ingested successfully!"