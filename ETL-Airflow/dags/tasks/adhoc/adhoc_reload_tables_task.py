# adhoc_20250713.py
# Import libraries
from airflow.decorators import task
from pyspark.sql import Row
import logging
from tasks.utils import (
    get_spark_session,
    write_into_table,
    abort_session,
    read_data,
    DuplicateChecker,
    APIClient,
    fetch_env_schema
)

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, coalesce, when, sum,
    current_date, current_timestamp, year, date_format,
    row_number, desc, count, lit, round, trim
)


# Create a task that helps in ingesting the data into Suppliers
@task(task_id="m_ingest_data_into_suppliers")
def supplier_data_ingestion(env, date, count):
    """
    Create a function to ingest into Suppliers

    Returns: The Success message for the load

    Raises: Duplicate Exception in case of any Duplicates
    """
    schema_dict = fetch_env_schema(env)
    raw = schema_dict['raw']
    legacy = schema_dict['legacy']

    # Create an object for the Suppliers API Client Class
    client = APIClient()

    # Get a spark session
    spark = get_spark_session()

    # Set the Suppliers value to fetch the data
    api = "suppliers"
    response = client.fetch_data(api, _date=date)

    # Create a data frame from the response of the API
    suppliers_df = spark.createDataFrame(Row(**x) for x in response['data'])
    logging.info("Data Frame : 'suppliers_df' is built")

    # Do the Transformations for the Suppliers Dataframe
    suppliers_df = suppliers_df \
        .withColumnRenamed(suppliers_df.columns[0], "SUPPLIER_ID") \
        .withColumnRenamed(suppliers_df.columns[1], "SUPPLIER_NAME") \
        .withColumnRenamed(suppliers_df.columns[2], "CONTACT_DETAILS") \
        .withColumnRenamed(suppliers_df.columns[3], "REGION")
    logging.info("Data Frame : Transformed 'suppliers_df' is built")

    # Populating the legacy version of Suppliers Data
    suppliers_df_lgcy = suppliers_df \
        .withColumn("DAY_DT", current_date() - count) \
        .select(
            col("DAY_DT"),
            col("SUPPLIER_ID"),
            col("SUPPLIER_NAME"),
            col("CONTACT_DETAILS"),
            col("REGION")
        )
    logging.info("Data Frame : 'suppliers_df_lgcy' is built")

    try:

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(suppliers_df, ['SUPPLIER_ID'])

        # Load the data into the tables
        write_into_table("suppliers_pre", suppliers_df, raw, "overwrite")
        write_into_table(api, suppliers_df_lgcy, legacy, "append")

    except Exception as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise

    finally:

        # Abort the session when Done.
        abort_session(spark)

    return f"{api} data ingested successfully!"


# Create a task that helps in ingesting the data into Customers
@task(task_id="m_ingest_data_into_customers")
def customer_data_ingestion(env, date, count):
    """
    Create a function to ingest into Customers

    Returns: The Success message for the load

    Raises: Duplicate Exception in case of any Duplicates
    """
    schema_dict = fetch_env_schema(env)
    raw = schema_dict['raw']
    legacy = schema_dict['legacy']

    # Create an object for the Suppliers API Client Class
    client = APIClient()

    # Get a spark session
    spark = get_spark_session()

    # Set the Customers value to fetch the data
    api = "customers"
    response = client.fetch_data(api, True, _date=date)

    # Create a data frame from the response of the API
    customer_df = spark.createDataFrame(Row(**x) for x in response['data'])
    logging.info("Data Frame : 'customer_df' is built")

    # Do the transformations for the customers Dataframe
    customer_df = customer_df \
        .withColumnRenamed(customer_df.columns[0], "CUSTOMER_ID") \
        .withColumnRenamed(customer_df.columns[1], "NAME") \
        .withColumnRenamed(customer_df.columns[2], "CITY") \
        .withColumnRenamed(customer_df.columns[3], "EMAIL") \
        .withColumnRenamed(customer_df.columns[4], "PHONE_NUMBER")
    logging.info("Data Frame : Transformed 'customer_df' is built")

    # Populating the legacy version of Customers Data
    customer_df_lgcy = customer_df \
        .withColumn("DAY_DT", current_date() - count) \
        .select(
            col("DAY_DT"),
            col("CUSTOMER_ID"),
            col("NAME"),
            col("CITY"),
            col("EMAIL"),
            col("PHONE_NUMBER")
        )
    logging.info("Data Frame : 'customer_df_lgcy' is built")

    try:

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(customer_df, ['CUSTOMER_ID'])

        # Load the data into the tables
        write_into_table("customers_pre", customer_df, raw, "overwrite")
        write_into_table(api, customer_df_lgcy, legacy, "append")

    except Exception as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise

    finally:

        # Abort the session when Done.
        abort_session(spark)

    return f"{api} data ingested successfully!"


# Create a task that helps in ingesting the data into Products
@task(task_id="m_ingest_data_into_products")
def products_data_ingestion(env, date, count):
    """
    Create a function to ingest into Products

    Returns: The Success message for the load

    Raises: Duplicate Exception in case of any Duplicates
    """
    schema_dict = fetch_env_schema(env)
    raw = schema_dict['raw']
    legacy = schema_dict['legacy']

    # Create an object for the Suppliers API Client Class
    client = APIClient()

    # Get a spark session
    spark = get_spark_session()

    # Set the Products value to fetch the data
    api = "products"
    response = client.fetch_data(api, _date=date)

    # Create a data frame from the response of the API
    product_df = spark.createDataFrame(Row(**x) for x in response['data'])
    logging.info("Data Frame : 'product_df' is built")

    # Do the Transformation for the product Dataframe
    product_df = product_df \
        .withColumnRenamed(product_df.columns[0], "PRODUCT_ID") \
        .withColumnRenamed(product_df.columns[1], "PRODUCT_NAME") \
        .withColumnRenamed(product_df.columns[2], "CATEGORY") \
        .withColumnRenamed(product_df.columns[3], "SELLING_PRICE") \
        .withColumnRenamed(product_df.columns[4], "COST_PRICE") \
        .withColumnRenamed(product_df.columns[5], "STOCK_QUANTITY") \
        .withColumnRenamed(product_df.columns[6], "REORDER_LEVEL") \
        .withColumnRenamed(product_df.columns[7], "SUPPLIER_ID")
    logging.info("Data Frame : Transformed 'product_df' is built")

    # Populating the legacy version of Products Data
    product_df_lgcy = product_df \
        .withColumn("DAY_DT", current_date() - count) \
        .select(
            col("DAY_DT"),
            col("PRODUCT_ID"),
            col("PRODUCT_NAME"),
            col("CATEGORY"),
            col("SELLING_PRICE"),
            col("COST_PRICE"),
            col("STOCK_QUANTITY"),
            col("REORDER_LEVEL"),
            col("SUPPLIER_ID")
        )
    logging.info("Data Frame : 'product_df_lgcy' is built")

    try:

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(product_df, ['PRODUCT_ID'])

        # Load the data into the tables
        write_into_table("products_pre", product_df, raw, "overwrite")
        write_into_table(api, product_df_lgcy, legacy, "append")

    except Exception as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise

    finally:

        # Abort the session when Done.
        abort_session(spark)

    return f"{api} data ingested successfully!"


# Create a task that helps the data in ingesting the data into sales
@task(task_id="m_ingest_data_into_sales")
def sales_data_ingestion(env, date, count):
    """
    Create a function to ingest into Sales

    Returns: The Success message for the load

    Raises: Duplicate Exception in case of any Duplicates
    """
    schema_dict = fetch_env_schema(env)
    raw = schema_dict['raw']
    legacy = schema_dict['legacy']

    today = date

    # Get the spark session
    spark = get_spark_session()

    # Create a data frame by reading the CSV from the Google Bucket
    sales_df = spark.read.csv(
                            f'gs://meta-morph-flow/{today}/sales_{today}.csv',
                            header=True,
                            inferSchema=True
                            )
    logging.info("Data Frame : 'sales_df' is built")

    api = "sales"
    logging.info("Reading the CSV File into dataframe...")

    # Do the Transformation for the Sales Dataframe
    sales_df = sales_df \
        .withColumnRenamed(sales_df.columns[0], "SALE_ID") \
        .withColumnRenamed(sales_df.columns[1], "CUSTOMER_ID") \
        .withColumnRenamed(sales_df.columns[2], "PRODUCT_ID") \
        .withColumnRenamed(sales_df.columns[3], "SALE_DATE") \
        .withColumnRenamed(sales_df.columns[4], "QUANTITY") \
        .withColumnRenamed(sales_df.columns[5], "DISCOUNT") \
        .withColumnRenamed(sales_df.columns[6], "SHIPPING_COST") \
        .withColumnRenamed(sales_df.columns[7], "ORDER_STATUS") \
        .withColumnRenamed(sales_df.columns[8], "PAYMENT_MODE")
    logging.info("Data Frame : Transformed 'sales_df' is built")

    # Populating the legacy version of Sales Data
    sales_df_lgcy = sales_df \
        .withColumn("DAY_DT", current_date() - count) \
        .select(
            col("DAY_DT"),
            col("SALE_ID"),
            col("CUSTOMER_ID"),
            col("PRODUCT_ID"),
            col("SALE_DATE"),
            col("QUANTITY"),
            col("DISCOUNT"),
            col("SHIPPING_COST"),
            col("ORDER_STATUS"),
            col("PAYMENT_MODE")
        )
    logging.info("Data Frame : 'sales_df_lgcy' is built")

    try:

        # Implement the Duplicate checker
        chk = DuplicateChecker()
        chk.has_duplicates(sales_df, ['SALE_ID'])

        # Load the data into the table
        write_into_table("sales_pre", sales_df, raw, "overwrite")
        write_into_table(api, sales_df_lgcy, legacy, "append")

    except Exception as e:

        # Raise an exception if Duplicates are found
        logging.error(str(e))
        raise

    finally:

        # Abort the session when Done.
        abort_session(spark)

    return f"{api} data ingested successfully!"


# Create a task that helps in Populating Suppliers Performance Table
@task(task_id="m_load_suppliers_performance")
def suppliers_performance_ingestion(env, date, cnt):
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
        .withColumn("day_dt", current_date() - cnt)
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


# Create a task that helps in populating Products_Performance
@task(task_id="m_load_products_performance")
def product_performance_ingestion(env, date, cnt):
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
        .withColumn("day_dt", current_date() - cnt) \
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


# Create a task that helps populating Customer Sales Report
@task(task_id="m_load_customer_sales_report")
def customer_sales_report_ingestion(env, date, cnt):
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
            (col("day_dt") == current_date() - cnt) &
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
        .withColumn("day_dt", current_date() - cnt) \
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
                        col("sale_date"), current_date() - cnt - 1
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
