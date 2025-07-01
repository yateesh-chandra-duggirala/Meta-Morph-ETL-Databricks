from airflow.decorators import task
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
from tasks.my_secrets import USERNAME, PASSWORD
from Raptor.Raptor import Raptor
from tasks.utils import get_spark_session

@task
def trigger_raptor():

    spark = get_spark_session()
    
    raptor_obj = Raptor(spark=spark,
                    username=USERNAME,
                    password=PASSWORD,)
    
    raptor_obj.submit_raptor_request(
        source_type='pg_admin',
        source_db='meta_morph',
        # target_type='reporting',
        target_type='pg_admin',
        target_db='meta_morph',
        source_sql="SELECT * FROM staging.customer_metrics_stg",
        target_sql="""SELECT * FROM staging.customer_metrics_sql_server""",
        email='yateed1437@gmail.com',
        output_table_name='customer_metrics',
        primary_key='CUSTOMER_ID'
    )

    return 'The Comparison Report is sent to the recipient..!'