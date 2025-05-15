# Import Libraries
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from tasks.my_secrets import PASSWORD, USERNAME, SERVICE_KEY

# Set the Level of logging to be INFO by default
logging.basicConfig(level=logging.INFO)

# Create a Class : APIClient to be used when ever API needs to be called
class APIClient:

    # Define the constructor to use the default URL
    def __init__(self, base_url="http://host.docker.internal:8000/v1"):
        self.base_url = base_url

    # Define a method to fetch the data when the API is called
    def fetch_data(self, api_type: str, auth=False):

        try :
            import requests

            # Auth is True means, The API is secured.
            if auth : 
                logging.info("Generating Token...")
                
                # Generate a token to get the Customer Data
                token = requests.get(self.base_url + "/token").json()['access_token']
                logging.info(f"The token is generated and fetching the response from {api_type} API")
                
                # Make the API call using Headers to overcome the Authentication Error response
                response = requests.get(self.base_url + "/" + api_type, headers={"Authorization": f"Bearer {token}"})
            
            else : 

                # Fetch the response directly from the API
                logging.info(f"Fetching the response from {api_type} API")
                response = requests.get(self.base_url + "/" + api_type)

            # Return the response as json if the Response is OK
            if response.status_code == 200:
                logging.info("Succcessfully retrieved response from API")
                return response.json()
                
        # Raise an exception in case of any failure
        except Exception as e:
            logging.error("Exception raised..!")
            raise e

# Custom Exception raised when duplicates are found in the dataset
class DuplicateException(Exception):

    def __init__(self, message='Duplicates are found in Dataset.'):
        super().__init__(message)

# Create a class named DuplicateChecker
class DuplicateChecker:

    # Create a class method for this class
    @classmethod
    def has_duplicates(cls, df, primary_key_list : list):
        logging.info("Checking for the duplicates in the provided Dataset")
        grouped_df = df.groupBy(primary_key_list) \
                            .agg(count('*').alias('cnt'))\
                            .filter('cnt > 1')
        if grouped_df.count() > 0 :
            raise DuplicateException
        logging.info("Duplicate Check successful.. No Duplicates found...")


# Define a function to get a spark session
def get_spark_session() :

    logging.info("Creating a Spark Session")

    # Create a spark session
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
    .config("spark.jars", "/usr/local/airflow/jars/postgresql-42.7.1.jar,/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", f"/usr/local/airflow/jars/{SERVICE_KEY}")
    logging.info("Spark session Created")
    return spark

# Define a function to read the data from the Postgres Database
def read_data(spark, table) :

    try :
        logging.info("Connecting to PostgreSQL database using JDBC driver...")
        df = spark.read.format("jdbc")\
            .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph")\
            .option("user", USERNAME)\
            .option("password", PASSWORD)\
            .option("driver", "org.postgresql.Driver")\
            .option("dbtable", table)\
            .load()
        logging.info(f"Retrieved Data from the {table}..")
    except Exception as e:
        logging.error("An Exception occurred")
        raise e
    return df

# This function is defined to Write the data into the PG Database
def write_into_table(table, data_frame, schema, strategy):

    try : 
        logging.info("Connecting to PostgreSQL database using JDBC driver...")
        logging.info(f"Established connection. Writing into {schema}.{table}")
        df = data_frame.write.format("jdbc")\
            .option("url", "jdbc:postgresql://host.docker.internal:5432/meta_morph") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", USERNAME) \
            .option("password", PASSWORD) \
            .mode(strategy) \
            .save()
        logging.info(f"Successfully Written {data_frame.count()} records into Table : {table}")
    except Exception as e:
        logging.error("An Exception occurred")
        raise e
    return df

# Define Function to abort the Spark Session used up to the instance
def abort_session(spark):
    spark.stop()
    logging.info("Spark Session ended.!")