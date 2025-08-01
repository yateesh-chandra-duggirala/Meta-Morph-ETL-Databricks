# Import Libraries
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
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
    def __init__(self, base_url="http://host.docker.internal:8000"):
        """
        Initializes the API Client with default or Provided base URL.

        Parameters :
        base_url (String): The base url of the API
        """
        self.base_url = base_url

    # Define a method to fetch the data when the API is called
    def fetch_data(self, api_type: str, auth=False, _date=None):
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
        try:
            import requests
            params = {}
            if _date:
                params['date'] = _date

            # Auth is True means, The API is secured.
            if auth:
                logging.info("Generating Token...")

                # Generate a token to get the Customer Data
                token = requests.get(
                    self.base_url + "/token"
                ).json()['access_token']
                logging.info(
                    f"The token generated. response fetched from {api_type}")

                # Make the API call using Headers for Authentication
                response = requests.get(
                    self.base_url + "/v2/" + api_type,
                    headers={"Authorization": f"Bearer {token}"},
                    params=params or None
                )

            else:

                # Fetch the response directly from the API
                logging.info(f"Fetching the response from {api_type} API")
                response = requests.get(self.base_url + "/v2/" + api_type,
                                        params=params or None)

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
    """This is the Class which contains the methods
    required for checking the Duplicates"""
    @classmethod
    def has_duplicates(cls, df, primary_key_list: list):
        """
        Checks for the Duplicates in the Dataframe
        based on the List of Primary key Columns provided.

        Parameters:
        df (Dataframe): The Spark Dataframe to check.
        primary_key_list (list): List of Primary Key Columns.

        Raises:
        Duplicate Exception if the Duplicates are found.
        """
        logging.info("Checking for the duplicates in the provided Dataset")
        grouped_df = df.repartition(
            4, *primary_key_list
        ).groupBy(primary_key_list) \
            .agg(count('*').alias('cnt'))\
            .filter('cnt > 1')
        if grouped_df.limit(1).count() > 0:
            raise DuplicateException
        logging.info("Duplicate Check successful.. No Duplicates found...")


# Define a function to get a spark session
def get_spark_session():
    """
    This Function returns the spark session
    """
    logging.info("Creating a Spark Session")
    SPARK_JARS = (
        "/usr/local/airflow/jars/postgresql-42.7.1.jar,"
        "/usr/local/airflow/jars/gcs-connector-hadoop3-latest.jar"
    )

    # Create a spark session
    spark = SparkSession.builder.appName("GCS_to_Postgres") \
        .master("local[*]") \
        .config("spark.jars", SPARK_JARS) \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.sql.session.timeZone", "Asia/Kolkata") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.maxResultSize", "512m") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.port", "4041") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile",
        f"/usr/local/airflow/jars/{SERVICE_KEY}"
    )
    logging.info("Spark session Created")
    return spark


# This function assigns the environment related schemas
def fetch_env_schema(env):
    if env == 'prod':
        logging.info('PRODUCTION Environment..!')
        result = {
            "raw": "raw",
            "legacy": "legacy",
            "staging": "staging",
            "gcs_legacy": "gs://reporting-lgcy"
        }
    else:
        logging.info('DEVELOPMENT Environment..!')
        result = {
            "raw": "dev_raw",
            "legacy": "dev_legacy",
            "staging": "dev_staging",
            "gcs_legacy": "gs://dev-reporting-lgcy"
        }
    return result


# Define a function named read_data
def read_data(spark, table):
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
    try:
        logging.info("Connecting to PostgreSQL database using JDBC driver...")
        df = spark.read.format("jdbc")\
            .option("url",
                    "jdbc:postgresql://host.docker.internal:5432/meta_morph")\
            .option("user", USERNAME)\
            .option("password", PASSWORD)\
            .option("numPartitions", 4) \
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
    try:
        logging.info("Connecting to PostgreSQL database using JDBC driver...")
        logging.info(f"Established connection. Writing into {schema}.{table}")
        df = data_frame.write.format("jdbc")\
            .option("url",
                    "jdbc:postgresql://host.docker.internal:5432/meta_morph")\
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", USERNAME) \
            .option("password", PASSWORD) \
            .mode(strategy) \
            .save()
        cnt = data_frame.count()
        logging.info(
            f"Successfully Written {cnt} records into Table : {table}"
        )
    except Exception as e:
        logging.error("An Exception occurred")
        raise e
    return df


# Define a function named write_to_gcs
def write_to_gcs(dataframe, gcs_path, location):
    """
    Writes the data into GCS Bucket Location

    Parameters:
    dataframe (Dataframe): The Spark Dataframe which we want to write
    location (String): The target GCS Location where to write
    """
    logging.info(
        f"Authenticating to {gcs_path} to load the data into parquet file.."
    )
    dataframe.repartition(2)\
        .write.mode("append").parquet(f"{gcs_path}/{location}")
    logging.info(f"Loaded into Parquet File : {location}")


# Define Function to abort the Spark Session used up to the instance
def abort_session(spark):
    """
    Stops the Spark Session taking itself as a parameter.
    """
    spark.stop()
    logging.info("Spark Session ended.!")


# Define function to fetch the list of tables from schema
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
            table_list.append(table[0].lower())

    except Exception as e:
        # Print the exception and allow the function to return an empty list
        logging.error(e)
        raise e

    finally:
        # Make sure connections get closed even if an exception happens
        try:
            cursor.close()
            connection.close()
        except Exception as e:
            raise e

    return table_list


# Executing Merge Statement on the PG Admin
def execute_merge(source_table: str, target_table: str):
    """
    Executes the Merge statement in the PG Admin

    Parameters
    ----------
    source_table : String
        The name of the PostgreSQL table which has to be upserted.

    Returns
    -------
    SUCCESSFUL EXECUTION.
    """

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
        logging.info("Created cursor out of the PG Connection..!")

        # Query information_schema for all tables in the specified schema
        cursor.execute(
            f"""
            MERGE INTO {target_table} as tgt
            USING {source_table} as src
            ON tgt."CUSTOMER_ID" = src."CUSTOMER_ID"
            WHEN MATCHED THEN
            UPDATE SET
                "CUSTOMER_NAME" = src."CUSTOMER_NAME",
                "TOTAL_ORDERS" = src."TOTAL_ORDERS",
                "TOTAL_AMOUNT_SAVINGS" = src."TOTAL_AMOUNT_SAVINGS",
                "TOTAL_SHIPPING_COST" = src."TOTAL_SHIPPING_COST",
                "EXPENDITURE" = src."EXPENDITURE",
                "AVERAGE_ORDER_VALUE" = src."AVERAGE_ORDER_VALUE",
                "FIRST_PURCHASE_DATE" = src."FIRST_PURCHASE_DATE",
                "LAST_PURCHASE_DATE" = src."LAST_PURCHASE_DATE",
                "MOST_USED_PAYMENT_MODE" = src."MOST_USED_PAYMENT_MODE",
                "DELIVERED_ORDERS_COUNT" = src."DELIVERED_ORDERS_COUNT",
                "CANCELLED_ORDERS_COUNT" = src."CANCELLED_ORDERS_COUNT",
                "ACTIVE_CUSTOMER_FLAG" = src."ACTIVE_CUSTOMER_FLAG",
                "CITY" = src."CITY",
                "EMAIL" = src."EMAIL",
                "PHONE_NUMBER" = src."PHONE_NUMBER",
                "UPDATE_TIMESTAMP" = src."UPDATE_TIMESTAMP"
            WHEN NOT MATCHED THEN
                INSERT (
                    "CUSTOMER_ID",
                    "CUSTOMER_NAME",
                    "TOTAL_ORDERS",
                    "TOTAL_AMOUNT_SAVINGS",
                    "TOTAL_SHIPPING_COST",
                    "EXPENDITURE",
                    "AVERAGE_ORDER_VALUE",
                    "FIRST_PURCHASE_DATE",
                    "LAST_PURCHASE_DATE",
                    "MOST_USED_PAYMENT_MODE",
                    "DELIVERED_ORDERS_COUNT",
                    "CANCELLED_ORDERS_COUNT",
                    "ACTIVE_CUSTOMER_FLAG",
                    "CITY",
                    "EMAIL",
                    "PHONE_NUMBER",
                    "LOAD_TIMESTAMP",
                    "UPDATE_TIMESTAMP"
                )
                VALUES (
                    src."CUSTOMER_ID",
                    src."CUSTOMER_NAME",
                    src."TOTAL_ORDERS",
                    src."TOTAL_AMOUNT_SAVINGS",
                    src."TOTAL_SHIPPING_COST",
                    src."EXPENDITURE",
                    src."AVERAGE_ORDER_VALUE",
                    src."FIRST_PURCHASE_DATE",
                    src."LAST_PURCHASE_DATE",
                    src."MOST_USED_PAYMENT_MODE",
                    src."DELIVERED_ORDERS_COUNT",
                    src."CANCELLED_ORDERS_COUNT",
                    src."ACTIVE_CUSTOMER_FLAG",
                    src."CITY",
                    src."EMAIL",
                    src."PHONE_NUMBER",
                    src."LOAD_TIMESTAMP",
                    src."UPDATE_TIMESTAMP"
                )
            """
        )
        logging.info("Merge executed Successfully..! Committing Transaction.")
        connection.commit()

    except Exception as e:
        # Print the exception and allow the function to return an empty list
        logging.error(e)
        raise e

    finally:
        # Make sure connections get closed even if an exception happens
        try:
            cursor.close()
            connection.close()
            logging.info("Committed and Corresponding Connections closed..!")
        except Exception as e:
            raise e

    return "Merge Done..!"
