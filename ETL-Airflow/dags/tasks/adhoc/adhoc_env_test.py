from airflow.decorators import task
import logging
from tasks.utils import (
    abort_session,
    get_spark_session
)

@task
def test_environment(env):
    if env == 'prod':   
        logging.info('PRODUCTION Environment')
    else :
        logging.info('DEVELOPMENT Environment')
    try:
        spark = get_spark_session()
        logging.info(env)
    except Exception as e:
        logging.error(e)
        raise
    finally:
        abort_session(spark)
    return f"Successfully fetched.."
