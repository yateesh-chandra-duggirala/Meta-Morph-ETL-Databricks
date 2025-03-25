from airflow.decorators import dag, task
from datetime import datetime
from pyspark.sql import SparkSession
import logging

@dag(
    dag_id="test_gcs",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def ingestion():

    @task(task_id="gcs_buck")
    def gcs_buck(api):
        # Create Spark session with required configurations
        spark = SparkSession.builder.appName("GCS_to_Postgres") \
            .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .getOrCreate()

        logging.info("Created Spark Session with GCS support.")

        # Set authentication for Google Cloud Storage
        spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/jars/meta-morph-d-eng-pro-admin.json")
        
        logging.info("Configured JSON authentication.")

        # Read CSV from GCS
        df = spark.read.csv('gs://meta-morph/20250323/sales_20250323.csv', header=True, inferSchema=True)
        logging.info("Dataframe read successfully.")
        df.show()

        return f"ETL Pipeline ingested {api} data successfully..!"

    gcs_buck("sales")

ingestion()
