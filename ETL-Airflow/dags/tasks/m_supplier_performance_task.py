from airflow.operators import task
import logging
from utils import get_spark_session, write_into_table, abort_session, read_data
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a task that helps in ingesting the data into Suppliers
@task(task_id="m_load_suppliers_performance")
def supplier_data_ingestion():

    # Get a spark session
    spark = get_spark_session()

    SQ_Shortcut_To_Suppliers = read_data(spark,"raw.suppliers")
    SQ_Shortcut_To_Suppliers = SQ_Shortcut_To_Suppliers \
                                .select(
                                    col("supplier_id"),
                                    col("supplier_name")
                                )
    
    SQ_Shortcut_To_Products = read_data(spark,"raw.products")
    SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
                                .select(
                                    col("product_id"),
                                    col("product_name"),
                                    col("supplier_id"),
                                    col("price")
                                )
    
    SQ_Shortcut_To_Sales = read_data("raw.sales")
    SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales \
                                .select(
                                    col("sale_id"),
                                    col("product_id"),
                                    col("order_status"),
                                    col("quantity"),
                                    col("discount")
                                )

    JNR_Supplier_Products = SQ_Shortcut_To_Suppliers \
                            .join(
                                SQ_Shortcut_To_Products, 
                                SQ_Shortcut_To_Suppliers.supplier_id == SQ_Shortcut_To_Products.supplier_id, 
                                'left'
                            ) \
                            .select(
                                SQ_Shortcut_To_Suppliers.supplier_id,
                                SQ_Shortcut_To_Suppliers.supplier_name,
                                SQ_Shortcut_To_Products.product_id,
                                SQ_Shortcut_To_Products.product_name,
                                SQ_Shortcut_To_Products.price
                            )
    
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
    
    AGG_TRANS = JNR_Master \
                            .groupBy(["supplier_id", "supplier_name", "product_name"]) \
                            .agg(
                                coalesce(
                                    sum(
                                    (col("price") - (col("price") * col("discount") / 100.0)) * col("quantity")
                                    ), lit(0.0)
                                ).alias("agg_total_revenue"),

                                count(col("sale_id")).alias("agg_total_products_sold"),
                                
                                coalesce(
                                    sum(
                                        col("quantity")
                                    ), lit(0)
                                ).alias("agg_total_stocks_sold")
                            )

    window_spec = Window.partitionBy(col("supplier_id")).orderBy(desc("agg_total_stocks_sold"))
    Shortcut_To_Suppliers_Performance_tgt = AGG_TRANS.withColumn("rnk", row_number().over(window_spec))

    Shortcut_To_Suppliers_Performance_tgt = Shortcut_To_Suppliers_Performance_tgt \
                                            .withColumn("performance_id", monotonically_increasing_id() + 1) \
                                            .filter("rnk = 1") \
                                            .drop(col("rnk"))

    Shortcut_To_Suppliers_Performance_tgt = Shortcut_To_Suppliers_Performance_tgt \
                                            .withColumnRenamed("performance_id","PERFORMANCE_ID") \
                                            .withColumnRenamed("supplier_id","SUPPLIER_ID") \
                                            .withColumnRenamed("supplier_name", "SUPPLIER_NAME") \
                                            .withColumnRenamed("agg_total_revenue", "TOTAL_REVENUE") \
                                            .withColumnRenamed("agg_total_products_sold", "TOTAL_PRODUCTS_SOLD") \
                                            .withColumnRenamed("agg_total_stocks_sold", "TOTAL_STOCKS_SOLD") \
                                            .withColumnRenamed("product_name", "TOP_PERFORMER")

    # Load the data into the table
    write_into_table("suppliers_performance", Shortcut_To_Suppliers_Performance_tgt, "raw", "overwrite")

    # Abort the session when Done.
    abort_session(spark)
    return f"suppliers_performance data ingested successfully!"