from airflow.decorators import dag, task
from datetime import datetime
from pyspark.sql import SparkSession, Row
import logging
from utils import get_api_call

# Create a DAG with No Schedule    
@dag(
    dag_id='ETL_US_Data',
    start_date=datetime(2025, 3, 8),
    schedule=None,
    catchup=False,
)
def my_dag():
        
    # Create A Task that stores Data from get_api_call function and 
    @task(task_id="load_into_pg")
    def read_data(res) :

        logging.info("Creating a Spark Session")
        
        # Create a Spark Session
        spark = SparkSession.builder.appName('Write_data_to_db')\
            .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
            .getOrCreate()
        logging.info("Spark session Created")

        # Read the data from the response data into Spark Dataframe
        df_data = spark.createDataFrame(Row(**x) for x in res['data'])
        logging.info("Data Frame Loaded.. Writing into Table")
        
        # Show the dataframe for debugging purpose
        df_data.show()

        # Writing data into Postgres Database
        df_data.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "raw.data_test") \
        .option("user", "postgres").option("password", "postgres") \
        .mode("overwrite") \
        .save()

        logging.info(f"Successfully Written {df_data.count()} records into Table : raw.data_test")

        spark.stop()
        logging.info("Successfully stopped Spark Session")
        return f"ETL Pipeline successfully Ran..!"
    
    res = get_api_call()
    read_data(res)

my_dag()