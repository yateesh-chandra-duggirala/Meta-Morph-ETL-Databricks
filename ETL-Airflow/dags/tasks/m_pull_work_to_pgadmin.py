from airflow.decorators import task
from airflow.models.xcom_arg import XComArg
from airflow.utils.task_group import TaskGroup
from .my_secrets import SERVICE_KEY
from tasks.utils import get_list_of_tables, get_spark_session, write_into_table
from google.cloud import storage
import logging


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
    df = spark.read.format("parquet").load(f"gs://raptor-workflow/{file}")
    table_name = file.split('/')[1].split('.')[-1]
    write_into_table(table_name, df, 'work', 'overwrite')


@task(task_id="m_pull_files")
def get_file_list() -> list[str]:
    """Return a list of files that still need loading."""
    files = list_files_to_load()
    if not files:
        logging.info("NO NEW FILES FOUND..!")
    else:
        logging.info("Found %s new files..!", len(files))
    return files
