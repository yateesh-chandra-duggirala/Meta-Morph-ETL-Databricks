from datetime import datetime
from airflow.decorators import dag
from tasks.m_pull_work_to_pgadmin import get_file_list, process_single_file


@dag(
    dag_id="PULL_DATA_INTO_PGADMIN",
    start_date=datetime(2025, 6, 1),
    schedule_interval=None,
    catchup=False,
    max_active_tasks=3,
    tags=["pgadmin"],
)
def pull_data_into_pgadmin():
    """
    Pull new raw files, then fan-out (map) to process each file individually.
    """
    # Task 1 – produce the iterable
    files_to_process = get_file_list()

    # Task 2 – dynamic task mapping / fan-out
    process_single_file.expand(file=files_to_process)


dag = pull_data_into_pgadmin()
