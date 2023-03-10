import pandas as pd
import numpy as np
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from sqlalchemy import create_engine, inspect
import psycopg2



class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self):

        extractor = DataExtractor()
        legacy_users_table = extractor.read_rds_table('legacy_users')

        legacy_users_table.replace('NULL', pd.NaT, inplace=True)
        legacy_users_table  = legacy_users_table.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=False)
        legacy_users_table.to_csv('legacy_users1.csv')



        legacy_users_table['date_of_birth'] = pd.to_datetime(legacy_users_table['date_of_birth'], errors = 'ignore')
        legacy_users_table['join_date'] = pd.to_datetime(legacy_users_table['join_date'], errors ='coerce')
        legacy_users_table = legacy_users_table.dropna(subset=['join_date'])


        legacy_users_table['phone_number'] = legacy_users_table['phone_number'].str.replace('/W', '')
        legacy_users_table = legacy_users_table.drop_duplicates(subset=['email_address'])
        return legacy_users_table 

instance = DataCleaning()
legacy_users_table = instance.clean_user_data()
legacy_users_table.to_csv('legacy_users2.csv')