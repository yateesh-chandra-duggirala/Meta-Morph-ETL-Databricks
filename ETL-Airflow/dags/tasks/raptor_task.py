from airflow.decorators import task
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
from tasks.my_secrets import USERNAME, PASSWORD
from Raptor.Raptor import Raptor
from tasks.utils import get_spark_session

@task
def trigger_raptor():

    spark = get_spark_session()
    
    raptor_obj = Raptor(spark=spark,
                    username=USERNAME,
                    password=PASSWORD,)
    
    raptor_obj.submit_raptor_request(
        source_type='pg_admin',
        source_db='meta_morph',
        # target_type='reporting',
        target_type='pg_admin',
        target_db='meta_morph',
        source_sql='SELECT "CUSTOMER_ID", COUNT(*) CNT FROM staging.customer_metrics_stg group by "CUSTOMER_ID" having count(*) > 1',
        target_sql="""
                       with CTE as (SELECT
                        c."CUSTOMER_ID",
                        c."NAME" AS "CUSTOMER_NAME",
                        c."CITY",
                        c."EMAIL",
                        c."PHONE_NUMBER",
                        COALESCE(SUM(s."QUANTITY"), 0) AS "TOTAL_ORDERS",
                        COALESCE(SUM(s."SHIPPING_COST"), 0) AS "TOTAL_SHIPPING_COST",
                        COALESCE(SUM(CASE WHEN s."ORDER_STATUS" = 'Delivered' THEN 1 END), 0) AS "DELIVERED_ORDERS_COUNT",
                        COALESCE(SUM(CASE WHEN s."ORDER_STATUS" = 'Cancelled' THEN 1 END), 0) AS "CANCELLED_ORDERS_COUNT",
                        COALESCE(SUM((s."QUANTITY" * p."SELLING_PRICE") * s."DISCOUNT" / 100), 0) AS "TOTAL_AMOUNT_SAVINGS",
                        COALESCE(SUM(s."QUANTITY" * p."SELLING_PRICE"), 0) AS "EXPENDITURE",
                        CASE 
                            WHEN SUM(s."QUANTITY") > 0 THEN ROUND(SUM(s."QUANTITY" * p."SELLING_PRICE")::numeric / SUM(s."QUANTITY"), 2)
                            ELSE 0
                        END AS "AVERAGE_ORDER_VALUE",
                        MIN(s."SALE_DATE") AS "FIRST_PURCHASE_DATE",
                        MAX(s."SALE_DATE") AS "LAST_PURCHASE_DATE",
                        CASE 
                            WHEN MAX(s."SALE_DATE") >= CURRENT_DATE - 4 THEN 'True'
                            ELSE 'False'
                        END AS "ACTIVE_CUSTOMER_FLAG",
                        (
                            SELECT s2."PAYMENT_MODE"
                            FROM legacy.sales s2
                            WHERE s2."CUSTOMER_ID" = c."CUSTOMER_ID"
                            GROUP BY s2."PAYMENT_MODE"
                            ORDER BY COUNT(*) DESC, s2."PAYMENT_MODE"
                            LIMIT 1
                        ) AS "MOST_USED_PAYMENT_MODE",
                        CURRENT_TIMESTAMP AS "LOAD_TIMESTAMP",
                        CURRENT_TIMESTAMP AS "UPDATE_TIMESTAMP"
                    FROM legacy.customers c
                    LEFT JOIN legacy.sales s ON s."CUSTOMER_ID" = c."CUSTOMER_ID"
                    LEFT JOIN legacy.products p ON p."PRODUCT_ID" = s."PRODUCT_ID"
                    GROUP BY c."CUSTOMER_ID", c."NAME", c."EMAIL", c."PHONE_NUMBER", c."CITY")
                    SELECT "CUSTOMER_ID", count(*) CNT from cte group by "CUSTOMER_ID" HAVING COUNT(*) > 1
                    """,
        email='yateed1437@gmail.com',
        output_table_name='customer_metrics_PK',
        primary_key='CUSTOMER_ID'
    )

    return 'The Comparison Report is sent to the recipient..!'