# Import Libraries
from airflow.decorators import task
import logging
from tasks.utils import (
    get_spark_session,
    write_into_table,
    abort_session,
    read_data,
    DuplicateChecker,
    fetch_env_schema
)
from pyspark.sql.functions import (
    col, trim, coalesce, sum, round,
    row_number, desc, current_date, count, lit
)
from pyspark.sql.window import Window


# Create a task that helps in Populating Suppliers Performance Table
@task(task_id="m_load_suppliers_performance")
def suppliers_performance_ingestion(env):
    """
    Create a function to load the Suppliers Performance

    Returns: The Success message for the load

    Raises: Duplicate exception if any Duplicates are found..
    """

    schema_dict = fetch_env_schema(env)
    raw = schema_dict['raw']
    legacy = schema_dict['legacy']

    # Get a spark session
    spark = get_spark_session()

    # Process the Node : SQ_Shortcut_To_Suppliers - reads from Suppliers_pre
    SQ_Shortcut_To_Suppliers = read_data(spark, f"{raw}.suppliers_pre")
    SQ_Shortcut_To_Suppliers = SQ_Shortcut_To_Suppliers \
        .select(
            col("supplier_id"),
            col("supplier_name")
        )
    logging.info("Data Frame : 'SQ_Shortcut_To_Suppliers' is built...")

    # Process the Node : SQ_Shortcut_To_Products - reads from Products_pre
    SQ_Shortcut_To_Products = read_data(spark, f"{raw}.products_pre")
    SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
        .select(
            col("product_id"),
            col("product_name"),
            col("supplier_id"),
            col("selling_price")
        )
    logging.info("Data Frame : 'SQ_Shortcut_To_Products' is built...")

    # Process the Node : SQ_Shortcut_To_Sales - reads from Sales_pre
    SQ_Shortcut_To_Sales = read_data(spark, f"{raw}.sales_pre")
    SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales \
        .select(
            col("sale_id"),
            col("product_id"),
            col("order_status"),
            col("quantity"),
            col("discount")
        )
    logging.info("Data Frame : 'SQ_Shortcut_To_Sales' is built...")

    # Process the Node : JNR_Supplier_Products - joins the 2 nodes
    JNR_Supplier_Products = SQ_Shortcut_To_Suppliers \
        .join(
            SQ_Shortcut_To_Products,
            trim(
                SQ_Shortcut_To_Suppliers.supplier_id
            ) == trim(SQ_Shortcut_To_Products.supplier_id),
            'left'
        ) \
        .select(
            SQ_Shortcut_To_Suppliers.supplier_id,
            SQ_Shortcut_To_Suppliers.supplier_name,
            SQ_Shortcut_To_Products.product_id,
            SQ_Shortcut_To_Products.product_name,
            SQ_Shortcut_To_Products.selling_price
        )
    logging.info("Data Frame : 'JNR_Supplier_Products' is built...")

    # Process the Node : JNR_Master - joins the 2 nodes
    JNR_Master = JNR_Supplier_Products.alias("jsp") \
        .join(
            SQ_Shortcut_To_Sales.alias("sls"),
            (col("sls.product_id") == col("jsp.product_id")) &
            (col("sls.order_status") != "Cancelled"),
            "left"
        ) \
        .select(
            col("jsp.supplier_id").alias("supplier_id"),
            col("jsp.supplier_name").alias("supplier_name"),
            col("jsp.product_name").alias("product_name"),
            col("jsp.selling_price").alias("selling_price"),
            col("sls.sale_id").alias("sale_id"),
            col("sls.order_status").alias("order_status"),
            col("sls.quantity").alias("quantity"),
            col("sls.discount").alias("discount")
        )
    logging.info("Data Frame : 'JNR_Master' is built...")

    # Process the Node : AGG_TRANS - Calculate the aggregates needed
    AGG_TRANS = JNR_Master \
        .groupBy(
            ["supplier_id"]
        ) \
        .agg(
            coalesce(round(sum((
                col("selling_price") - (
                    col("selling_price") * col("discount") / 100.0
                ))
                * col("quantity")
                ), 2), lit(0.0)
            ).alias("agg_total_revenue"),

            count(col("sale_id")).alias("agg_total_products_sold"),

            coalesce(
                round(sum(
                    col("quantity")
                ), 2), lit(0)
            ).alias("agg_total_stocks_sold")
        )
    logging.info("Data Frame : 'AGG_TRANS' is built...")

    # Process the Node : EXP_Product_Revenue - Join to get all suppliers
    EXP_Product_Revenue = JNR_Master \
        .withColumn(
            "product_revenue",
            (
                col("selling_price") * col("quantity")
            ) * (1 - col("discount") / 100.0)
        ) \
        .groupBy("supplier_id", "product_name") \
        .agg(
            round(sum("product_revenue"), 2).alias("product_revenue")
        )
    logging.info("Data Frame : 'EXP_Product_Revenue' is built...")

    # Define the window spec to rank products by revenue per supplier
    window_spec = Window.partitionBy(
        "supplier_id"
        ).orderBy(desc("product_revenue"))

    # Process the Node : RNK_Revenue - Rank over Revenue
    RNK_Revenue = EXP_Product_Revenue \
        .withColumn("rnk", row_number().over(window_spec)) \
        .filter(col("rnk") == 1) \
        .withColumnRenamed("product_name", "top_product") \
        .select("supplier_id", "top_product")
    logging.info("Data Frame : 'RNK_Revenue' is built...")

    # Process the Node : JNR_Suppliers_Performance - Joins dataframes
    JNR_Suppliers_Performance = AGG_TRANS.alias("agg") \
        .join(
            SQ_Shortcut_To_Suppliers.alias("s"),
            trim(col("agg.supplier_id")) == trim(col("s.supplier_id")),
            "left"
        ) \
        .join(
            RNK_Revenue.alias("rr"),
            trim(col("agg.supplier_id")) == trim(col("rr.supplier_id")),
            "left"
        ) \
        .select(
            col("agg.supplier_id").alias("supplier_id"),
            col("s.supplier_name").alias("supplier_name"),
            col("agg.agg_total_revenue").alias("total_revenue"),
            col("agg.agg_total_products_sold").alias("total_products_sold"),
            col("agg.agg_total_stocks_sold").alias("total_stocks_sold"),
            col("rr.top_product").alias("top_product")
        ) \
        .withColumn("day_dt", current_date())
    logging.info("Data Frame : 'JNR_Suppliers_Performance' is built...")

    # Process the Node : Shortcut_To_Suppliers_Performance_tgt - The Target
    Shortcut_To_Suppliers_Performance_tgt = JNR_Suppliers_Performance \
        .select(
                col("day_dt").alias("DAY_DT"),
                col("supplier_id").alias("SUPPLIER_ID"),
                col("supplier_name").alias("SUPPLIER_NAME"),
                col("total_revenue").alias("TOTAL_REVENUE"),
                col("total_products_sold").alias("TOTAL_PRODUCTS_SOLD"),
                col("total_stocks_sold").alias("TOTAL_STOCK_SOLD"),
                col("top_product").alias("TOP_SELLING_PRODUCT")
        )
    logging.info(
        "Data Frame : 'Shortcut_To_Suppliers_Performance_tgt' is built..."
    )

    try:

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(
            Shortcut_To_Suppliers_Performance_tgt,
            ['DAY_DT', 'SUPPLIER_ID']
        )

        # Load the data into the table
        write_into_table(
            table="supplier_performance",
            data_frame=Shortcut_To_Suppliers_Performance_tgt,
            schema=legacy,
            strategy="append"
        )

    except Exception as e:

        # Raise an exception
        logging.error(str(e))
        raise

    finally:

        # Abort the session when Done.
        abort_session(spark)

    return "supplier_performance data ingested successfully!"
