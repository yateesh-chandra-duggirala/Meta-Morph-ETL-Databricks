from airflow.decorators import task
from .my_secrets import SERVICE_KEY
from tasks.utils import get_list_of_tables, get_spark_session, write_into_table
from google.cloud import storage
import logging
from pyspark.errors import AnalysisException


def list_files_to_load():
    """
    Function to check the files that are required to be ingested.
    """
    client = storage.Client.from_service_account_json(
        f"/usr/local/airflow/jars/{SERVICE_KEY}")
    bucket = client.get_bucket("raptor-workflow")
    blobs = bucket.list_blobs()
    file_list = [blob.name for blob in blobs if blob.name.endswith('/')]
    logging.info("Fetched files from the raptor Workflow bucket...")
    tables = get_list_of_tables('work')
    files_to_be_loaded = [
        file for file in file_list
        if file.split('/')[1].split('.')[-1].lower() not in tables
    ]
    return files_to_be_loaded


@task
def process_single_file(file: str):
    """
    Consumes the files from the File list one by one and loads into table
    """
    spark = get_spark_session()
    try:
        df = spark.read.format("parquet").load(f"gs://raptor-workflow/{file}")
        if df.count() == 0:
            logging.info(f"Skipping {file}")
        else:
            logging.info(f"Processing {file}")
            table_name = file.split('/')[1].split('.')[-1]
            write_into_table(table_name, df, 'work', 'overwrite')
    except AnalysisException as e:
        logging.info(f"Skipping Temp parquet File with exception{str(e)}..!")
    return f"{file} successfully pulled into Local postgres"


@task(task_id="m_pull_files")
def get_file_list() -> list[str]:
    """Return a list of files that still need loading."""
    files = list_files_to_load()
    if not files:
        logging.info("NO NEW FILES FOUND..!")
    else:
        logging.info("Found %s new files..!", len(files))
    return files
