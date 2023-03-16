import pandas as pd
import numpy as np

from sqlalchemy import create_engine, inspect
import psycopg2

from data_extraction import DataExtractor
from database_utils import DatabaseConnector



class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, legacy_users_table):
     
        legacy_users_table.replace('NULL', pd.NaT, inplace=True)
        legacy_users_table  = legacy_users_table.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=False)

        legacy_users_table['date_of_birth'] = pd.to_datetime(legacy_users_table['date_of_birth'], errors = 'ignore')
        legacy_users_table['join_date'] = pd.to_datetime(legacy_users_table['join_date'], errors ='coerce')
        legacy_users_table = legacy_users_table.dropna(subset=['join_date'])


        legacy_users_table['phone_number'] = legacy_users_table['phone_number'].str.replace('/W', '')
        legacy_users_table = legacy_users_table.drop_duplicates(subset=['email_address'])
        return legacy_users_table 




if __name__ == "__main__":
    # Creates class instances
    extractor = DataExtractor()
    connector = DatabaseConnector()
    cleaner = DataCleaning()

    db_creds = connector.read_db_creds()
    engine = connector.init_db_engine(db_creds)
    table_names = connector.list_db_tables(engine)

    legacy_users_table = extractor.read_rds_table(table_names, 'legacy_users', engine)
    clean_legacy_users_table = cleaner.clean_user_data(legacy_users_table)
    connector.upload_to_db(clean_legacy_users_table, "dim_users", db_creds)


