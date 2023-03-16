import pandas as pd
import numpy as np
import yaml
from sqlalchemy import create_engine, inspect
import psycopg2

class DataExtractor:
    def __init__(self):
        pass


    def read_rds_table(self, table_names, table_name, engine):

        engine = engine.connect()

        data = pd.read_sql_table(table_name, engine)
        return data # it will take in an instance of the database connector class and the table name as an argument and return a pd df
        # Use list_db_tables method to get the name of the table containing user data
        # Use read_rds_table method to extract the table containing user data and return a pd df
        



