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
    col, coalesce, when, sum,
    current_date, current_timestamp, year, date_format
)


# Create a task that helps populating Customer Sales Report
@task(task_id="m_load_customer_sales_report")
def customer_sales_report_ingestion(env):
    """
    Create a function to load the Customer Sales Report

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
            col("category"),
            col("selling_price")
        )
    logging.info("Data Frame : 'SQ_Shortcut_To_Products' is built...")

    # Process the Node : SQ_Shortcut_To_Sales - reads data from Sales_pre
    SQ_Shortcut_To_Sales = read_data(spark, f"{raw}.sales_pre")
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
    logging.info("Data Frame : 'SQ_Shortcut_To_Sales' is built...")

    # Process the Node : SQ_Shortcut_To_Customers - reads from Customers_pre
    SQ_Shortcut_To_Customers = read_data(spark, f"{raw}.customers_pre")
    SQ_Shortcut_To_Customers = SQ_Shortcut_To_Customers \
        .select(
            col("customer_id"),
            col("name"),
            col("city")
        )
    logging.info("Data Frame : 'SQ_Shortcut_To_Customers' is built...")

    # Fetching list of the top_selling_products from the Supplier Performance
    SQ_Shortcut_To_Supplier_Performance = read_data(
        spark,
        f"{legacy}.supplier_performance"
    )
    SQ_Shortcut_To_Supplier_Performance = SQ_Shortcut_To_Supplier_Performance\
        .select(
            col("top_selling_product")
        ) \
        .filter(
            (col("day_dt") == current_date()) &
            (col("top_selling_product").isNotNull())
        ).distinct()
    data_list = SQ_Shortcut_To_Supplier_Performance.collect()
    top_selling_products = [row['top_selling_product'] for row in data_list]

    # Process the Node : JNR_Sales_Customer - joins the 2 nodes
    JNR_Sales_Customer = SQ_Shortcut_To_Customers \
        .join(
            SQ_Shortcut_To_Sales,
            (SQ_Shortcut_To_Sales.customer_id ==
             SQ_Shortcut_To_Customers.customer_id
             ) &
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
    logging.info("Data Frame : 'JNR_Sales_Customer' is built...")

    # Process the Node : JNR_Master - joins the 2 nodes
    JNR_Master = JNR_Sales_Customer \
        .join(
            SQ_Shortcut_To_Products,
            (
                JNR_Sales_Customer.product_id ==
                SQ_Shortcut_To_Products.product_id
             ),
            "inner"
        ) \
        .select(
            JNR_Sales_Customer.sale_id,
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
            SQ_Shortcut_To_Products.selling_price
        )
    logging.info("Data Frame : 'JNR_Master' is built...")

    # Process the Node : EXP_Add_Sales_Data - Adding sales comprising columns
    EXP_Add_Sales_Data = JNR_Master \
        .withColumn("day_dt", current_date()) \
        .withColumn("price",
                    col(
                        "selling_price"
                    ) - col("selling_price") * col("discount") / 100) \
        .withColumn("sale_amount",
                    col(
                        "quantity"
                    )*col(
                        "selling_price"
                    )*(1-(col("discount")/100))
                    ) \
        .withColumn("sale_date",
                    coalesce(
                        col("sale_date"), current_date() - 1
                    )) \
        .withColumn("sale_year",
                    year(col("sale_date"))
                    ) \
        .withColumn("sale_month",
                    date_format(col("sale_date"), "MMMM")
                    ) \
        .withColumn("load_tstmp", current_timestamp()) \
        .withColumn(
            "top_performer",
            when(
                col("product_name").isin(top_selling_products),
                True
            )
            .otherwise(False)
        )
    logging.info("Data Frame : 'EXP_Add_Sales_Data' is built...")

    # Process the Node : AGG_TRANS_Customer - Calculate the aggregates.
    AGG_TRANS_Customer = EXP_Add_Sales_Data \
        .groupBy("customer_id") \
        .agg(
            sum("sale_amount").alias("agg_sales_amount")
        )
    logging.info("Data Frame : 'AGG_TRANS_Customer' is built...")

    # Calculate the limits of the tier level
    quantiles = AGG_TRANS_Customer.approxQuantile(
        "agg_sales_amount",
        [0.5, 0.8], 0.01
    )
    silver_tier = quantiles[0]
    gold_tier = quantiles[1]

    # Process the Node : EXP_Customer_Sales_Report - Add Loyalty_Tier column
    EXP_Customer_Sales_Report = AGG_TRANS_Customer \
        .withColumn(
            "loyalty_tier",
            when(col("agg_sales_amount") > gold_tier, "GOLD")
            .when(
                col("agg_sales_amount").between(silver_tier, gold_tier),
                "SILVER"
            )
            .otherwise("BRONZE")
        )
    logging.info("Data Frame : 'EXP_Customer_Sales_Report' is built...")

    # Process the Node : JNR_Sales_Customer_Report - joins the nodes
    JNR_Sales_Customer_Report = EXP_Add_Sales_Data.alias("a") \
        .join(
            EXP_Customer_Sales_Report.alias("b"),
            col("a.customer_id") == col("b.customer_id"),
            "inner"
        ) \
        .select(
            col("a.sale_id"),
            col("a.order_status"),
            col("a.quantity"),
            col("a.discount"),
            col("a.sale_date"),
            col("a.shipping_cost"),
            col("b.customer_id"),
            col("a.name"),
            col("a.city"),
            col("a.product_id"),
            col("a.product_name"),
            col("a.category"),
            col("a.price"),
            col("a.day_dt"),
            col("a.sale_amount"),
            col("a.sale_year"),
            col("a.sale_month"),
            col("b.loyalty_tier"),
            col("a.top_performer"),
            col("a.load_tstmp")
        )
    logging.info("Data Frame : 'JNR_Sales_Customer_Report' is built...")

    # Prodcess the Node : Shortcut_To_CSR_Tgt - The Target
    Shortcut_To_CSR_Tgt = JNR_Sales_Customer_Report \
        .select(
            col("day_dt").alias("DAY_DT"),
            col("customer_id").alias("CUSTOMER_ID"),
            col("name").alias("CUSTOMER_NAME"),
            col("sale_id").alias("SALE_ID"),
            col("city").alias("CITY"),
            col("product_name").alias("PRODUCT_NAME"),
            col("category").alias("CATEGORY"),
            col("sale_date").alias("SALE_DATE"),
            col("sale_month").alias("SALE_MONTH"),
            col("sale_year").alias("SALE_YEAR"),
            col("quantity").alias("QUANTITY"),
            col("price").alias("PRICE"),
            col("sale_amount").alias("SALE_AMOUNT"),
            col("loyalty_tier").alias("LOYALTY_TIER"),
            col("top_performer").alias("TOP_PERFORMER"),
            col("load_tstmp").alias("LOAD_TSTMP")
        )
    logging.info("Data Frame : 'Shortcut_To_CSR_Tgt' is built...")

    try:

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(
            Shortcut_To_CSR_Tgt,
            ['DAY_DT', 'SALE_ID']
        )

        # Load the data into the table
        write_into_table(
            table="customer_sales_report",
            data_frame=Shortcut_To_CSR_Tgt,
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

    return "customer_sales_report data ingested successfully!"
