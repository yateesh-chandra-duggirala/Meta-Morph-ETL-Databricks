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
    col, coalesce, when, sum, round, current_date, lit
)


# Create a task that helps in populating Products_Performance
@task(task_id="m_load_products_performance")
def product_performance_ingestion(env):
    """
    Create a function to load the Products Performance

    Returns: The Success message for the load

    Raises: Duplicate exception if any Duplicates are found..
    """

    schema_dict = fetch_env_schema(env)
    raw = schema_dict['raw']
    legacy = schema_dict['legacy']

    # Get a spark session
    spark = get_spark_session()

    # Process the Node : SQ_Shortcut_To_Products - reads from Products_pre
    SQ_Shortcut_To_Products = read_data(spark, f"{raw}.products_pre")
    SQ_Shortcut_To_Products = SQ_Shortcut_To_Products \
        .select(
            col("product_id"),
            col("product_name"),
            col("selling_price"),
            col("cost_price"),
            col("category"),
            col("stock_quantity"),
            col("reorder_level")
        )
    logging.info("Data Frame : 'SQ_Shortcut_To_Products' is built...")

    # Process the Node : SQ_Shortcut_To_Sales - reads from Sales_pre Table
    SQ_Shortcut_To_Sales = read_data(spark, f"{raw}.sales_pre")
    SQ_Shortcut_To_Sales = SQ_Shortcut_To_Sales \
        .select(
            col("product_id"),
            col("order_status"),
            col("quantity"),
            col("discount")
        )
    logging.info("Data Frame : 'SQ_Shortcut_To_Sales' is built...")

    # Process the Node : JNR_Master - joins the 2 nodes
    JNR_Master = SQ_Shortcut_To_Products \
        .join(
            SQ_Shortcut_To_Sales,
            (
                SQ_Shortcut_To_Sales.product_id ==
                SQ_Shortcut_To_Products.product_id
             ) &
            (SQ_Shortcut_To_Sales.order_status != "Cancelled"),
            "left"
        ) \
        .select(
            SQ_Shortcut_To_Products.product_id,
            SQ_Shortcut_To_Products.product_name,
            SQ_Shortcut_To_Products.selling_price,
            SQ_Shortcut_To_Products.cost_price,
            SQ_Shortcut_To_Products.category,
            SQ_Shortcut_To_Products.stock_quantity,
            SQ_Shortcut_To_Products.reorder_level,
            SQ_Shortcut_To_Sales.order_status,
            SQ_Shortcut_To_Sales.quantity,
            SQ_Shortcut_To_Sales.discount
        )
    logging.info("Data Frame : 'JNR_Master' is built...")

    # Process the Node : AGG_TRANS - Calculate the aggregates
    AGG_TRANS = JNR_Master \
        .groupBy(
            [
                "product_id", "product_name", "category", "stock_quantity",
                "reorder_level", "cost_price"
             ]
        ) \
        .agg(
            coalesce(
                round(sum((
                    col("selling_price") - (
                        col("selling_price") * col("discount") / lit(100.0)
                    )
                ) * col("quantity")
                ), 2), lit(0.0)
            ).alias("agg_total_sales_amount"),

            when(
                sum(col("quantity")) > lit(0),
                round(coalesce(sum((
                    col("selling_price") - (
                        col("selling_price") * col("discount") / lit(100.0)
                    )
                ) * col("quantity")
                        ) / sum(col("quantity")),
                        lit(0.0)
                    ), 2
                )
            )
            .otherwise(
                lit(0.0)
            ).alias("agg_average_sale_price"),

            coalesce(
                sum(col("quantity")), lit(0)
            ).alias("agg_total_quantity_sold")
        )
    logging.info("Data Frame : 'AGG_TRANS' is built...")

    # Assigns a rank to each product based on their product_id
    EXP_Products_Performance_tgt = AGG_TRANS \
        .withColumn(
            "total_stocks_left",
            col("stock_quantity") - col("agg_total_quantity_sold")
        ) \
        .withColumn(
            "reordered_quantity",
            col("reorder_level") * col("stock_quantity") / 100
        ) \
        .withColumn(
            "stock_level_status",
            when(
                col("total_stocks_left") < col("reordered_quantity"),
                "Below Reorder Level"
            ).otherwise("Sufficient Stock")
        ) \
        .withColumn("day_dt", current_date()) \
        .withColumn(
            "profit",
            coalesce(round(
                col("agg_total_sales_amount") - (
                    col("agg_total_quantity_sold") * col("cost_price")
                ), 2
            ), lit(0.0))
        )
    logging.info("Data Frame : 'EXP_Products_Performance_tgt' is built...")

    # Process the Node : Shortcut_To_Products_Performance_tgt - The Target
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
    logging.info(
        "Data Frame : 'Shortcut_To_Products_Performance_tgt' is built"
    )

    try:

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(
            Shortcut_To_Products_Performance_tgt,
            ['DAY_DT', 'PRODUCT_ID']
        )

        # Load the data into the table
        write_into_table(
            table="product_performance",
            data_frame=Shortcut_To_Products_Performance_tgt,
            schema=legacy,
            strategy="append"
        )

    except Exception as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise

    finally:

        # Abort the session when Done.
        abort_session(spark)

    return "product_performance data ingested successfully!"
