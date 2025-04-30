# Import Libraries
from airflow.decorators import task
import logging
from utils import get_spark_session, write_into_table, abort_session, read_data, DuplicateChecker, DuplicateException
from pyspark.sql.functions import *

# Create a task that helps in ingesting the data into Suppliers
@task(task_id="m_load_products_performance")
def product_performance_ingestion():

    # Get a spark session
    spark = get_spark_session()

    # Process the Node : SQ_Shortcut_To_Products - reads data from Products Table
    SQ_Shortcut_To_Products = read_data(spark,"raw.products")
    SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                .select(
                                    col("product_id"),
                                    col("product_name"),
                                    col("price"),
                                    col("cost_price"),
                                    col("category"),
                                    col("stock_quantity"),
                                    col("reorder_level")
                                )
    logging.info(f"Data Frame : 'SQ_Shortcut_To_Products' is built...")

    # Process the Node : SQ_Shortcut_To_Sales - reads data from Sales Table
    SQ_Shortcut_To_Sales = read_data(spark,"raw.sales")
    SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales \
                                .select(
                                    col("product_id"),
                                    col("order_status"),
                                    col("quantity"),
                                    col("discount")
                                )
    logging.info(f"Data Frame : 'SQ_Shortcut_To_Sales' is built...")

    # Process the Node : JNR_Master - joins the 2 nodes and JNR_Supplier_Products and SQ_Shortcut_To_Sales
    JNR_Master = SQ_Shortcut_To_Products \
                            .join(
                                SQ_Shortcut_To_Sales,
                                (SQ_Shortcut_To_Sales.product_id == SQ_Shortcut_To_Products.product_id) & 
                                (SQ_Shortcut_To_Sales.order_status != "Cancelled"),
                                "left"
                            ) \
                            .select(
                                SQ_Shortcut_To_Products.product_id,
                                SQ_Shortcut_To_Products.product_name,
                                SQ_Shortcut_To_Products.price,
                                SQ_Shortcut_To_Products.cost_price,
                                SQ_Shortcut_To_Products.category,
                                SQ_Shortcut_To_Products.stock_quantity,
                                SQ_Shortcut_To_Products.reorder_level,
                                SQ_Shortcut_To_Sales.order_status,
                                SQ_Shortcut_To_Sales.quantity,
                                SQ_Shortcut_To_Sales.discount
                            )
    logging.info(f"Data Frame : 'JNR_Master' is built...")

    # Process the Node : AGG_TRANS - Calculate the aggregates that are needed for the target columns
    AGG_TRANS = JNR_Master \
                    .groupBy(["product_id", "product_name", "category", "stock_quantity", "reorder_level", "cost_price"]) \
                    .agg(
                        coalesce(
                            round(sum(
                            (col("price") - (col("price") * col("discount") / lit(100.0))) * col("quantity")
                            ),2), lit(0.0)
                        ).alias("agg_total_sales_amount"),

                        coalesce(
                            round(avg(col("price")), 2), lit(0.0)
                        ).alias("agg_average_sale_price"),

                        coalesce(sum(col("quantity")), lit(0)).alias("agg_total_quantity_sold")
                    )
    logging.info(f"Data Frame : 'AGG_TRANS' is built...")

    # Assigns a rank to each product based on their product_id
    EXP_Products_Performance_tgt = AGG_TRANS \
                                            .withColumn(
                                                "total_stocks_left", col("stock_quantity") - col("agg_total_quantity_sold")
                                            ) \
                                            .withColumn(
                                                "reordered_quantity", col("reorder_level") * col("stock_quantity") / 100
                                            ) \
                                            .withColumn(
                                                "stock_level_status", 
                                                    when(col("total_stocks_left") < col("reordered_quantity"), "Below Reorder Level").otherwise("Sufficient Stock")
                                            ) \
                                            .withColumn("day_dt", current_date()) \
                                            .withColumn(
                                                "profit",
                                                coalesce(
                                                    round(
                                                        col("agg_total_sales_amount") - (col("agg_total_quantity_sold") * col("cost_price")),
                                                        2
                                                    ), lit(0.0)
                                                )
                                            )
    logging.info(f"Data Frame : 'EXP_Products_Performance_tgt' is built...")

    # Process the Node : Shortcut_To_Products_Performance_tgt - The Target desired table
    Shortcut_To_Products_Performance_tgt = EXP_Products_Performance_tgt \
                                            .select(
                                                col("day_dt").alias("DAY_DT"),
                                                col("product_id").alias("PRODUCT_ID"),
                                                col("product_name").alias("PRODUCT_NAME"),
                                                col("agg_total_sales_amount").alias("TOTAL_SALES_AMOUNT"),
                                                col("agg_total_quantity_sold").alias("TOTAL_QUANTITY_SOLD"),
                                                col("stock_quantity").alias("STOCK_QUANTITY"),
                                                col("agg_average_sale_price").alias("AVG_SALE_PRICE"),
                                                col("reorder_level").alias("REORDER_LEVEL"),
                                                col("stock_level_status").alias("STOCK_LEVEL_STATUS"),
                                                col("profit").alias("PROFIT"),
                                                col("category").alias("CATEGORY")
                                            )

    logging.info("Data Frame : 'Shortcut_To_Products_Performance_tgt' is built")

    try :

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(Shortcut_To_Products_Performance_tgt, ['DAY_DT','PRODUCT_ID'])
        
        # Load the Data into Parquet File
        logging.info("Authenticating to GCS to load the data into parquet file..")
        Shortcut_To_Products_Performance_tgt.write.mode("append").parquet("gs://reporting-legacy/product_performance")
        logging.info(f"Loaded into Parquet File : product_performance")

        # Load the data into the table
        write_into_table("product_performance", Shortcut_To_Products_Performance_tgt, "legacy", "append")        

    except DuplicateException as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise
    
    finally :

        # Abort the session when Done.
        abort_session(spark)

    return f"product_performance data ingested successfully!"