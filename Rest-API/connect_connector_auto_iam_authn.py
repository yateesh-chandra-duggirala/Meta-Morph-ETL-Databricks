from google.cloud.sql.connector import Connector
import pg8000
import google.auth
import pg8000.dbapi
import sqlalchemy

GOOGLE_APPLICATION_CREDENTIALS = "ece-data-eng-service-creds.json"

def connect_with_connector_auto_iam_authn() -> sqlalchemy.engine.base.Engine:

    instance_connection_name = "ece-data-eng-project:us-central1:meta-morph-things"
    db_user_name = "meta_admin"  
    db_user_password = "db_handler1210"
    db_name = "morph_store"

    credentials, project = google.auth.load_credentials_from_file(GOOGLE_APPLICATION_CREDENTIALS)

    # initialize Cloud SQL Python Connector object
    connector = Connector(credentials=credentials)

    def getconn() -> pg8000.dbapi.Connection:
        print(credentials)
        print(project)
        return connector.connect(
            instance_connection_name,
            driver="pg8000",  
            user=db_user_name,
            password=db_user_password,
            db=db_name,
        )

    # The Cloud SQL Python Connector can be used with SQLAlchemy
    # using the 'creator' argument to 'create_engine'
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        pool_size=10,
        max_overflow=2,
        pool_timeout=300,
        pool_recycle=1800,
    )
    return pool