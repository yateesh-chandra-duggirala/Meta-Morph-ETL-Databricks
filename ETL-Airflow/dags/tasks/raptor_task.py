from airflow.decorators import task
from tasks.my_secrets import USERNAME, PASSWORD
from Raptor.Raptor import Raptor
from tasks.utils import get_spark_session


@task
def trigger_raptor():

    spark = get_spark_session()

    raptor_obj = Raptor(
        spark=spark,
        username=USERNAME,
        password=PASSWORD,
    )

    raptor_obj.submit_raptor_request(
        source_type='pg_admin',
        source_db='meta_morph',
        target_type='reporting',
        source_sql="""
                    SELECT * FROM legacy.customer_sales_report
                    """,
        target_sql="""
                    SELECT * FROM reporting.customer_sales_report
                    """,
        email='yateed1437@gmail.com',
        output_table_name='customer_sales_report',
        primary_key='SALE_ID,DAY_DT'
    )

    return 'The Comparison Report is sent to the recipient..!'
