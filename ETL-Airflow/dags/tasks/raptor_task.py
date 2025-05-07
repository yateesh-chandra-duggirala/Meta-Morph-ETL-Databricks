from airflow.decorators import task
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
from .my_secrets import USERNAME, PASSWORD
from .raptor_util import Raptor
from pyspark.sql import SparkSession
import logging

@task
def trigger_raptor():

    # Create the spark session
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/jars/meta-morph-d-eng-pro-admin.json")
    logging.info("Spark session Created")

    raptor = Raptor(spark=spark,
                    username=USERNAME,
                    password=PASSWORD,)
    
    raptor.submit_raptor_request(
        source_type='pg_admin',
        source_db='meta_morph',
        target_type='reporting',
        source_sql="SELECT * FROM legacy.supplier_performance",
        target_sql="SELECT * FROM reporting.supplier_performance ",
        email='yateed1437@gmail.com',
        output_table_name_suffix='CUSTOMER_SALES_REPORT_LGCY_VS_REPORT',
        primary_key='SUPPLIER_ID,DAY_DT'
    )

    return 'The Comparison Report is sent to the recipient..!'