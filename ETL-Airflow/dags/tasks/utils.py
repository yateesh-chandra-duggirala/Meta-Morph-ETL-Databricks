# Import Libraries
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from tasks.my_secrets import PASSWORD, USERNAME, SERVICE_KEY
import psycopg2
from typing import List

# Set the Level of logging to be INFO by default
logging.basicConfig(level=logging.INFO)


# Create a Class : APIClient to be used when ever API needs to be called
class APIClient:
    """
    A Client Class to interact with APIs.

    Attributes:
    base_url (String): The base URL of the API Requests.
    """
    def __init__(self, base_url="http://host.docker.internal:8000/v1"):
        """
        Initializes the API Client with default or Provided base URL.

        Parameters : 
        base_url (String): The base url of the API
        """
        self.base_url = base_url

    # Define a method to fetch the data when the API is called
    def fetch_data(self, api_type: str, auth=False):
        """
        Fetches data from the API Endpoint.

        Parameters:
        api_type (String): The API endpoint to call.
        auth (Boolean): Whether the authentication is needed.

        Returns:
        response (Dictionary): The JSON response from the API.

        Raises: 
        Exception: If the API Call fails.
        """
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
    """Custom Exception Raised when Duplicates are found in the Dataset"""
    def __init__(self, message='Duplicates are found in Dataset.'):
        super().__init__(message)


# Create a class named DuplicateChecker
class DuplicateChecker:
    """This is the Class which contains the methods required for checking the Duplicates"""
    @classmethod
    def has_duplicates(cls, df, primary_key_list : list):
        """
        Checks for the Duplicates in the Dataframe based on the List of Primary key Columns provided.

        Parameters:
        df (Dataframe): The Spark Dataframe to check.
        primary_key_list (list): List of column names to be used as the Primary Keys.

        Raises:
        Duplicate Exception if the Duplicates are found.
        """
        logging.info("Checking for the duplicates in the provided Dataset")
        grouped_df = df.groupBy(primary_key_list) \
                            .agg(count('*').alias('cnt'))\
                            .filter('cnt > 1')
        if grouped_df.count() > 0 :
            raise DuplicateException
        logging.info("Duplicate Check successful.. No Duplicates found...")


# Define a function to get a spark session
def get_spark_session() :
    """
    This Function returns the spark session
    """
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


# Define a function named read_data
def read_data(spark, table) :
    """
    Reads the data from the Postgres Database

    Parameters:
    spark (Spark Session): The Spark session with the required Jars
    table (String): Reading the Table using JDBC from the PG Admin

    Returns: 
    The Dataframe built on by reading data from the table.

    Raises:
    Exception: In case of Any Failure while reading from the table.
    """
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


# Define a function named write_into_table
def write_into_table(table, data_frame, schema, strategy):
    """
    Writes the data into the Postgres Table

    Parameters:
    data_frame (Dataframe): The Spark Dataframe
    table (String): The Table where we want to write the data_frame into Table
    schema (String): The Schema where we want to store the table
    strategy (String): The type of the insertion into table (append/overwrite)

    Returns: 
    The Dataframe built after writing data into the table
    """
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


# Define a function named write_to_gcs
def write_to_gcs(dataframe, location):
    """
    Writes the data into GCS Bucket Location

    Parameters:
    dataframe (Dataframe): The Spark Dataframe which we want to write
    location (String): The target GCS Location where to write
    """
    logging.info("Authenticating to GCS to load the data into parquet file..")
    dataframe.write.mode("append").parquet(f"gs://reporting-lgcy/{location}")
    logging.info(f"Loaded into Parquet File : {location}")


# Define Function to abort the Spark Session used up to the instance
def abort_session(spark):
    """
    Stops the Spark Session taking itself as a parameter.
    """
    spark.stop()
    logging.info("Spark Session ended.!")


def get_list_of_tables(schema: str) -> List[str]:
    """
    Return a list of table names that live inside a given PostgreSQL schema.

    Parameters
    ----------
    schema : String
        The name of the PostgreSQL schema whose tables you want to inspect.

    Returns
    -------
    list[String]
        A list containing the table names found in *schema*.  
        If an error occurs, an empty list is returned and the error is printed.
    """
    table_list: List[str] = []

    try:
        # Establish a connection to the database
        connection = psycopg2.connect(
            host="host.docker.internal",
            database="meta_morph",
            user="postgres",
            password=PASSWORD,
            port="5432",
        )

        # Open a cursor to perform database operations
        cursor = connection.cursor()

        # Query information_schema for all tables in the specified schema
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            """,
            (schema,),  # parameterised to prevent SQL injection
        )

        # Fetch results and populate the return list
        tables = cursor.fetchall()
        print("Available tables list:")
        for table in tables:
            table_list.append(table[0])

    except Exception as e:
        # Print the exception and allow the function to return an empty list
        logging.error(e)

    finally:
        # Make sure connections get closed even if an exception happens
        try:
            cursor.close()
            connection.close()
        except Exception:
            pass

    return table_list