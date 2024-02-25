import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    def __init__(self) -> None:
        pass  # No initialization logic required
    def read_db_creds(self):
        # Read the database credentials from a YAML file
        with open('db_creds.yaml', 'r') as stream:
            return yaml.safe_load(stream)
    def init_db_engine(self):
        # Initialize the database engine using credentials from YAML file
        data_loaded = self.read_db_creds()
        return create_engine(f'postgresql+psycopg2://{data_loaded["RDS_USER"]}:{data_loaded["RDS_PASSWORD"]}@{data_loaded["RDS_HOST"]}:{data_loaded["RDS_PORT"]}/{data_loaded["RDS_DATABASE"]}')

    def list_db_tables(self):
        # List tables in the database
        return inspect(self.init_db_engine()).get_table_names()
    
    def upload_to_db(self, df, table):
        # Upload DataFrame to specified table in the database
        data_loaded = self.read_db_creds()
        # Create engine for database connection
        engine = create_engine(f'postgresql+psycopg2://{data_loaded["USER"]}:{data_loaded["PASSWORD"]}@{data_loaded["HOST"]}/{data_loaded["DBNAME"]}') # DB credentials stored in yaml credentials file
        # Upload DataFrame to database
        df.to_sql(name=table, con=engine)

