from airflow.decorators import task
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
from tasks.my_secrets import USERNAME, PASSWORD
from tasks.Raptor_MOD.Raptor import Raptor
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
        target_type='reporting',
        source_sql="""
                    SELECT "DAY_DT", "SUPPLIER_ID", "SUPPLIER_NAME", "TOTAL_REVENUE", "TOTAL_PRODUCTS_SOLD", "TOTAL_STOCK_SOLD" FROM dev_legacy.supplier_performance
                    """,
        target_sql="""
                    SELECT "DAY_DT", "SUPPLIER_ID", "SUPPLIER_NAME", "TOTAL_REVENUE", "TOTAL_PRODUCTS_SOLD", "TOTAL_STOCK_SOLD" FROM reporting.supplier_performance
                    """,
        email='asritha.vig2338@gmail.com',
        output_table_name='supplier_performance',
        primary_key='SUPPLIER_ID,DAY_DT'
    )

    return 'The Comparison Report is sent to the recipient..!'