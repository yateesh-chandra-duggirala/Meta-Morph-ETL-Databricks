# Import Libraries
from airflow.decorators import task
import logging
from tasks.utils import (
    get_spark_session,
    write_into_table,
    abort_session,
    read_data,
    DuplicateChecker
)
from pyspark.sql.functions import (
    col, trim, coalesce, when, sum, round,
    row_number, desc, current_date, count, lit
)
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

    # Process the Node : SQ_Shortcut_To_Suppliers - reads from Suppliers_pre
    SQ_Shortcut_To_Suppliers = read_data(spark, "raw.suppliers_pre")
    SQ_Shortcut_To_Suppliers = SQ_Shortcut_To_Suppliers \
        .select(
            col("supplier_id"),
            col("supplier_name")
        )
    logging.info("Data Frame : 'SQ_Shortcut_To_Suppliers' is built...")

    # Process the Node : SQ_Shortcut_To_Products - reads from Products_pre
    SQ_Shortcut_To_Products = read_data(spark, "raw.products_pre")
    SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
        .select(
            col("product_id"),
            col("product_name"),
            col("supplier_id"),
            col("selling_price")
        )
    logging.info("Data Frame : 'SQ_Shortcut_To_Products' is built...")

    # Process the Node : SQ_Shortcut_To_Sales - reads from Sales_pre
    SQ_Shortcut_To_Sales = read_data(spark, "raw.sales_pre")
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

    # Process the Node : JNR_ALL - Join to get all suppliers
    JNR_ALL = JNR_Master.alias('mstr') \
        .join(
            AGG_TRANS.alias('exps'),
            trim(col("mstr.supplier_id")) == trim(col("exps.supplier_id")),
            "left"
        ) \
        .select(
            col("mstr.supplier_id").alias("supplier_id"),
            col("mstr.supplier_name").alias("supplier_name"),
            col("exps.agg_total_revenue").alias("agg_total_revenue"),
            col("exps.agg_total_products_sold").alias(
                "agg_total_products_sold"
            ),
            col("exps.agg_total_stocks_sold").alias(
                "agg_total_stocks_sold"
            ),
            col("mstr.product_name").alias("product_name")
        )
    logging.info("Data Frame : 'JNR_ALL' is built...")

    # Process the Node : EXP_Suppliers_Performance - Assigns rank
    window_spec = Window.partitionBy(
        col("supplier_id")
    ).orderBy(
        desc("agg_total_revenue"),
        desc("agg_total_stocks_sold")
    )
    EXP_Suppliers_Performance = JNR_ALL \
        .withColumn(
            "rnk", row_number().over(window_spec)
        ) \
        .filter("rnk = 1") \
        .withColumn(
            "product_name",
            when(
                col("agg_total_revenue") > 0,
                col("product_name")
            )
            .otherwise(lit(None).cast("string"))
        ) \
        .drop(col("rnk")) \
        .withColumn("day_dt", current_date())
    logging.info("Data Frame : 'EXP_Suppliers_Performance' is built...")

    # Process the Node : Shortcut_To_Suppliers_Performance_tgt - The Target
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
            schema="dev_legacy",
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
