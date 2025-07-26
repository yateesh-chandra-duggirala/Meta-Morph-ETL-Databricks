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
        # target_type='reporting',
        target_type='pg_admin',
        target_db='meta_morph',
        source_sql="""
                    SELECT * FROM legacy.supplier_performance
                    WHERE "DAY_DT" >= '2025-07-22'
                    """,
        target_sql="""
                    SELECT * FROM dev_legacy.supplier_performance
                    WHERE "DAY_DT" >= '2025-07-22'
                    """,
        email='yateed1437@gmail.com',
        output_table_name='supplier_performance',
        primary_key='DAY_DT,SUPPLIER_ID'
    )

    return 'The Comparison Report is sent to the recipient..!'
