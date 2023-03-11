import pandas as pd
import numpy as np
from database_utils import DatabaseConnector
from sqlalchemy import create_engine, inspect
import psycopg2

class DataExtractor:
    def __init__(self):
        pass


    def read_rds_table(self, table_name):
        connector = DatabaseConnector()
        engine = connector.init_db_engine(connector.read_db_creds())
        tables = connector.list_db_tables(engine)
        engine = engine.connect()

        user_data = pd.read_sql_table(table_name, engine)
        return user_data # it will take in an instance of the database connector class and the table name as an argument and return a pd df
        # Use list_db_tables method to get the name of the table containing user data
        # Use read_rds_table method to extract the table containing user data and return a pd df
        



instance = DataExtractor()
legacy_users_table = instance.read_rds_table('legacy_users')
legacy_users_table.to_csv('.legacy_users.csv')