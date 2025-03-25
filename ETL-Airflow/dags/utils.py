from airflow.decorators import task, dag
import logging
from pyspark.sql import SparkSession, Row


class APIClient:
    
    def __init__(self, base_url="http://host.docker.internal:8000/v1"):
        self.base_url = base_url

    def fetch_data(self, api_type: str):
        import requests
        url = f"{self.base_url}/{api_type}"
        response = requests.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data, status code: {response.status_code}")


def get_spark_session() :

    logging.info("Creating a Spark Session")
    spark = SparkSession.builder.appName('Write_data_to_db')\
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
        .getOrCreate()
    logging.info("Spark session Created")
    return spark

def load_into_table(api, data_frame):
    df = data_frame.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph") \
    .option("driver", "org.postgresql.Driver").option("dbtable", f"raw.{api}") \
    .option("user", "postgres").option("password", "postgres") \
    .mode("overwrite") \
    .save()

    logging.info(f"Successfully Written {data_frame.count()} records into Table : {api}")
    return df

def abort_session(spark):
    spark.stop()
    logging.info("Spark Session ended.!")