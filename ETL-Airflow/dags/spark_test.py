from airflow.decorators import dag, task
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
from utils import get_api_call
import logging
import os
jar_path = os.path.abspath("./jars/postgresql-42.7.1.jar")
    
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def my_dag():
        
    @task
    def read_data(res) :

        logging.info("Creating a Spark Session")
        spark = SparkSession.builder.appName('Write_data_to_db')\
            .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
            .getOrCreate()
        logging.info("Spark session Created")
        spark

        df_data = spark.createDataFrame(Row(**x) for x in res['data'])
        logging.info("Data Frame Loaded.. Writing into Table")
        df_data.show()
        df_data.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "data_test") \
        .option("user", "postgres").option("password", "postgres") \
        .mode("overwrite") \
        .save()

        logging.info(f"Successfully Written {df_data.count()} records into Table : data_usa")
            
        return f"ETL Pipeline successfully Ran..!"
    
    read_data(get_api_call())

my_dag()