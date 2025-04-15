import logging
from pyspark.sql import SparkSession

class APIClient:
    
    def __init__(self, base_url="http://host.docker.internal:8000/v1"):
        self.base_url = base_url

    def fetch_data(self, api_type: str, auth=False):
        import requests
        if auth : 
            logging.info("Generating Token...")
            token = requests.get(self.base_url + "/token").json()['access_token']
            logging.info(f"The token is generated and fetching the response from {api_type} API")
            response = requests.get(self.base_url + "/customers", headers={"Authorization": f"Bearer {token}"})
        else : 
            logging.info(f"Fetching the response from {api_type} API")
            response = requests.get(self.base_url + "/" + api_type)

        if response.status_code == 200:
            logging.info("Succcessfully retrieved response from API")
            return response.json()
        else:
            logging.error("Exception raised..!")
            raise Exception(f"Failed to fetch data, status code: {response.status_code}")


def get_spark_session() :

    logging.info("Creating a Spark Session")
    spark = SparkSession.builder.appName('Write_data_to_db')\
        .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar") \
        .getOrCreate()
    logging.info("Spark session Created")
    return spark

def read_data(spark,table) :

    df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/meta_morph")\
        .option("user", "postgres")\
        .option("password", "postgres")\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", table)\
        .load()
    
    return df

def write_into_table(api, data_frame, schema, strategy):
    df = data_frame.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph") \
    .option("driver", "org.postgresql.Driver").option("dbtable", f"{schema}.{api}") \
    .option("user", "postgres").option("password", "postgres") \
    .mode(strategy) \
    .save()

    logging.info(f"Successfully Written {data_frame.count()} records into Table : {api}")
    return df

def abort_session(spark):
    spark.stop()
    logging.info("Spark Session ended.!")