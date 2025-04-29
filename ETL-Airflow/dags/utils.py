import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logging.basicConfig(level=logging.INFO)

class APIClient:
    
    def __init__(self, base_url="http://host.docker.internal:8000/v1"):
        self.base_url = base_url

    def fetch_data(self, api_type: str, auth=False):
        try :
            import requests
            if auth : 
                logging.info("Generating Token...")
                token = requests.get(self.base_url + "/token").json()['access_token']
                logging.info(f"The token is generated and fetching the response from {api_type} API")
                response = requests.get(self.base_url + "/" + api_type, headers={"Authorization": f"Bearer {token}"})
            else : 
                logging.info(f"Fetching the response from {api_type} API")
                response = requests.get(self.base_url + "/" + api_type)

            if response.status_code == 200:
                logging.info("Succcessfully retrieved response from API")
                return response.json()
                
        except Exception as e:
            logging.error("Exception raised..!")
            raise e

# Custom Exception raised when duplicates are found in the dataset
class DuplicateException(Exception):

    def __init__(self, message='Duplicates found in DataFrame.'):
        super().__init__(message)

# Create a class named DuplicateChecker
class DuplicateChecker:

    @classmethod
    def has_duplicates(cls, df, primary_key_list : list):
        grouped_df = df.groupBy(primary_key_list) \
                            .agg(count('*').alias('cnt'))\
                            .filter('cnt > 1')
        if grouped_df.count() > 0 :
            raise DuplicateException


def get_spark_session() :

    logging.info("Creating a Spark Session")
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
    .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/jars/meta-morph-d-eng-pro-admin.json")
    logging.info("Spark session Created")
    return spark

def read_data(spark, table) :

    try :
        logging.info("Connecting to PostgreSQL database using JDBC driver...")
        df = spark.read.format("jdbc")\
            .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph")\
            .option("user", "postgres")\
            .option("password", "postgres")\
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable", table)\
            .load()
        logging.info(f"Retrieved Data from the {table}..")
    except Exception as e:
        logging.error("An Exception occurred")
        raise e
    return df

def write_into_table(table, data_frame, schema, strategy):

    try : 
        logging.info("Connecting to PostgreSQL database using JDBC driver...")
        logging.info(f"Established connection. Writing into {schema}.{table}")
        df = data_frame.write.format("jdbc")\
            .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .mode(strategy) \
            .save()
        logging.info(f"Successfully Written {data_frame.count()} records into Table : {table}")
    except Exception as e:
        logging.error("An Exception occurred")
        raise e
    return df

def abort_session(spark):
    spark.stop()
    logging.info("Spark Session ended.!")