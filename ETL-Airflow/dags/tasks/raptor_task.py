from airflow.decorators import task
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *
from .my_secrets import USERNAME, PASSWORD
from .raptor_util import Raptor

@task
def trigger_raptor():

    raptor = Raptor(username=USERNAME,
                    password=PASSWORD,)
    
    raptor.submit_raptor_request(
        source_type='pg_admin',
        source_db='meta_morph',
        target_type='reporting',
        source_sql="SELECT * FROM legacy.supplier_performance",
        target_sql="SELECT * FROM reporting.supplier_performance ",
        email='yateed1437@gmail.com',
        output_table_name_suffix='CUSTOMER_SALES_REPORT_LGCY_VS_REPORT',
        primary_key='SUPPLIER_ID,DAY_DT'
    )

    return 'The Comparison Report is sent to the recipient..!'